import logging
import argparse
import pathlib

from statcan.client import StatCan, Language, MetadataDatabase

logger = logging.getLogger(__name__)


def add_root_arguments(parser: argparse.ArgumentParser):
    parser.add_argument(
        "-v",
        "--verbose",
        action="count",
        default=0,
        help="increase verbosity",
    )
    parser.add_argument(
        "--polars",
        action="store_true",
        help="Use polars instead of pandas. Note: This requires 'polars' extra be installed.",
    )
    parser.add_argument(
        "-n",
        "--return-rows",
        default=None,
        dest="return_rows",
        type=int,
        help="Number of rows to return (default is whatever df.head() returns)",
    )

def add_download_arguments(parser: argparse.ArgumentParser):
    parser.add_argument(
        "-l",
        "--language",
        default=Language.ENGLISH,
        choices=(Language.ENGLISH, Language.FRENCH),
        help="The language for the dataset"
    )
    parser.add_argument(
        type=str,
        dest="table_number",
        metavar="TABLE_NUMBER",
        help="The hyphenated table number",
    )
    parser.add_argument(
        "--save-dir",
        type=pathlib.Path,
        default=None,
        help="Save the resulting CSV file to this directory.",
    )

def add_search_arguments(parser: argparse.ArgumentParser):
    parser.add_argument(
        metavar="KEYWORD",
        nargs="+",
        dest="keywords",
        help="The keywords to search for",
    )
    parser.add_argument(
        "-d",
        "--database-path",
        type=str,
        default=".statcan_db.sqlite",
        help="The path to write the sqlite database of StatCAN catalogue to.",
    )


def add_subparsers(parser: argparse.ArgumentParser):
    subparsers = parser.add_subparsers(
        title="command",
        dest="command",
        help="The two main ways of consuming the StatCAN datasets."
    )

    search_parser = subparsers.add_parser("search")
    download_parser = subparsers.add_parser("download")

    add_root_arguments(parser)
    add_download_arguments(download_parser)
    add_search_arguments(search_parser)


def _set_up_logging(verbosity: int):
    logging_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    if not verbosity:
        levels = {
            "httpx": logging.ERROR,
            "httpcore": logging.ERROR,
            "hishel": logging.INFO,
            "statcan": logging.INFO
        }
    elif verbosity == 1:
        levels = {
            "httpx": logging.DEBUG,
            "httpcore": logging.ERROR,
            "hishel": logging.DEBUG,
            "statcan": logging.DEBUG
        }
    else:
        levels = {
            "httpx": logging.DEBUG,
            "httpcore": logging.DEBUG,
            "hishel": logging.DEBUG,
            "statcan": logging.DEBUG
        }

    logging.basicConfig(
        level=logging.DEBUG,
        format=logging_format,
    )

    for logger_name, log_level in levels.items():
        logging.getLogger(logger_name).setLevel(log_level)


def main():
    parser = argparse.ArgumentParser(
        description="Download wrangled datasets for Pandas or Polars from "
                    "StatCAN just like how https://github.com/warint/statcanR does it in R.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    add_subparsers(parser)
    opts = parser.parse_args()

    _set_up_logging(opts.verbose)

    client = StatCan()

    if opts.command == "search":
        db = MetadataDatabase(path=opts.database_path or ":memory:")
        db.load()
        df = db.search(*opts.keywords)
        print(df.head(n=opts.return_rows))
        return

    if opts.command == "download":
        csv = client.download(opts.table_number, opts.language, save_dir=opts.save_dir)
        if opts.polars:
            df = csv.get_df_polars()
        else:
            df = csv.get_df_pandas()
        print(df.head(n=opts.return_rows))
        return


if __name__ == '__main__':
    main()
