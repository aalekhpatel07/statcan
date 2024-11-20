import csv
import dataclasses
import pathlib
import typing
from functools import cached_property

import logging
import enum
import io
import zipfile
from pathlib import Path
import httpx

_has_polars = False
_has_pandas = False

try:
    import polars as pl
    _has_polars = True
except ImportError:
    pass

try:
    import pandas as pd
    _has_pandas = True
except ImportError:
    pass


def _assert_has_polars():
    if not _has_polars:
        raise ImportError("polars is required but not found. Please install the 'polars' extra with 'pip install statcan[polars]'")

def _assert_has_pandas():
    if not _has_pandas:
        raise ImportError("pandas is required but not found. Please install the 'pandas' extra with 'pip install statcan[pandas]'")


logger = logging.getLogger(__name__)


class Language(enum.Enum):
    ENGLISH = "en"
    FRENCH = "fr"

    def __str__(self) -> str:
        return self.value

    def url_for(self, table_number: str) -> str:
        if self == Language.ENGLISH:
            return f"https://www150.statcan.gc.ca/n1/{self}/tbl/csv/{table_number}-eng.zip"
        else:
            return f"https://www150.statcan.gc.ca/n1/{self}/tbl/csv/{table_number}-{self}.zip"


@dataclasses.dataclass
class CSVContents:
    """Represents the CSV contents and metadata returned for some dataset.

    """
    table: bytes
    metadata: bytes

    @cached_property
    def dataset_name(self) -> str:
        """Get the name of the dataset from the metadata contents.

        :return:
        """
        lines = iter(self.metadata.split(b"\n", maxsplit=2))
        next(lines)
        second = next(lines).decode("utf-8")
        reader = csv.reader([second])
        return next(reader)[0]


    def get_df_polars(self):
        """Load the wrangled CSV contents as a Polars Dataframe.

        :return:
        """

        _assert_has_polars()

        df = pl.read_csv(source=self.table, has_header=True)
        dataset_name = self.dataset_name

        columns = [
            pl.lit(dataset_name).alias("INDICATOR"),
        ]
        if "COORDINATE" in df.columns:
            columns.append(
                (pl.col("COORDINATE").cast(pl.String)).alias("COORDINATE")
            )

        ref_date = str(df[0, 0])
        if len(ref_date) == 4:
            columns.append(
                (pl.format("{}-1-1", 'REF_DATE').cast(pl.Date)).alias("REF_DATE")
            )
        elif len(ref_date) == 7:
            columns.append(
                (pl.format("{}-1", 'REF_DATE').cast(pl.Date)).alias("REF_DATE")
            )
        elif len(ref_date) == 9:
            columns.append(
                ((pl.col("REF_DATE").str.replace(r".*/(\d{4})", "${1}-3-31")).cast(pl.Date)).alias("REF_DATE")
            )
            columns.append(
                pl.lit("Fiscal Year").alias("REF_PERIOD")
            )

        return df.with_columns(*columns)

    def get_df_pandas(self):
        """Load the wrangled CSV contents as a Pandas Dataframe.

        :return:
        """
        _assert_has_pandas()
        df = pd.read_csv(io.BytesIO(self.table))

        df["INDICATOR"] = self.dataset_name
        if "COORDINATE" in df.columns:
            df["COORDINATE"] = df["COORDINATE"].astype(str)

        ref_date = str(df.iloc[0, 0])
        if len(ref_date) == 4:
            # 2021
            df["REF_DATE"] = df["REF_DATE"].apply(lambda s: f'{s}-1-1')
            df["REF_DATE"] = pd.to_datetime(df["REF_DATE"])
        elif len(ref_date) == 7:
            # 2021-01
            df["REF_DATE"] = df["REF_DATE"].apply(lambda s: f'{s}-1')
            df["REF_DATE"] = pd.to_datetime(df["REF_DATE"])
        elif len(ref_date) == 9:
            df["REF_DATE"] = df["REF_DATE"].str.replace(r".*/", "", regex=True)
            df["REF_DATE"] = df["REF_DATE"].apply(lambda s: f'{s}-3-31')
            df["REF_PERIOD"] = "Fiscal year"
            df["REF_DATE"] = pd.to_datetime(df["REF_DATE"])
        else:
            df["REF_DATE"] = df["REF_DATE"].apply(lambda s: f'{s}')
            df["REF_DATE"] = pd.to_datetime(df["REF_DATE"])
        return df

    def get_prepared_csv(self) -> bytes:
        bio = io.BytesIO()
        if _has_pandas:
            df = self.get_df_pandas()
            df.to_csv(bio, index=False)  # noqa
            return bio.getvalue()
        if _has_polars:
            df = self.get_df_polars()
            df.write_csv(bio)
            return bio.getvalue()
        raise ValueError(
            "Neither polars nor pandas backends are available to wrangle the csv. "
            "Please install one with 'pip install statcan[polars]' or 'pip install statcan[pandas]'"
        )


class StatCan:

    def __init__(self):
        try:
            import hishel

            controller = hishel.Controller(
                cacheable_methods=["GET"],
                cacheable_status_codes=[200],
                allow_stale=True,
                always_revalidate=True,
            )
            storage = hishel.FileStorage(
                base_path=Path(".cache")
            )
            self.client = hishel.CacheClient(controller=controller, storage=storage)
            logger.debug("hishel client initialized. Responses may be cached.")
        except ImportError:
            self.client = httpx.Client()
            logger.debug("hishel (http response cacher) is not installed. Responses may not be cached.")

    def download(self,
                 table_number: str,
                 language: Language,
                 save_dir: typing.Optional[pathlib.Path] = None,
                 ) -> CSVContents:
        table_number = table_number[:-2].replace("-", "")
        response = self.client.get(
            language.url_for(table_number),
            timeout=httpx.Timeout(300, pool=None, connect=None),
        )
        response.raise_for_status()
        zipped_contents = io.BytesIO(response.content)
        with zipfile.ZipFile(
            zipped_contents,
            "r",
            zipfile.ZIP_DEFLATED,
            False
        ) as zipped_contents:
            contents = zipped_contents.read(f"{table_number}.csv")
            metadata = zipped_contents.read(f"{table_number}_MetaData.csv")
            csv_contents = CSVContents(table=contents, metadata=metadata)

        if save_dir:
            if not save_dir.is_dir():
                logger.error("save_dir is not a directory, not saving contents to a csv file.")
                return csv_contents
            save_path = save_dir / f"statcan_{table_number}_{language}.csv"
            logger.info(f"Saving to {save_path}")
            with open(str(save_path), "w") as f:
                f.write(csv_contents.get_prepared_csv().decode("utf-8"))

        return csv_contents

    def search(self):
        pass