import logging
import argparse
import pathlib

from statcan.client import StatCan, Language

logger = logging.getLogger(__name__)


def add_arguments(parser: argparse.ArgumentParser):
    parser.add_argument(
        "-v",
        "--verbose",
        action="count",
        default=0,
        help="increase verbosity",
    )
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
        "--polars",
        action="store_true",
        help="Use polars instead of pandas. Note: This requires 'polars' extra be installed.",
    )
    parser.add_argument(
        "--save-dir",
        type=pathlib.Path,
        default=None,
        help="Save the resulting CSV file to this directory.",
    )


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
        description="StatCan CLI"
    )
    add_arguments(parser)
    opts = parser.parse_args()

    _set_up_logging(opts.verbose)

    client = StatCan()
    csv = client.download(opts.table_number, opts.language, save_dir=opts.save_dir)
    if opts.polars:
        df = csv.get_df_polars()
    else:
        df = csv.get_df_pandas()
    print(df.head())

if __name__ == '__main__':
    main()
