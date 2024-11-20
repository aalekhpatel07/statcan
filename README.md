# StatCAN (dataset fetcher and wrangler for Python)

Does exactly what [statCanR](https://cran.r-project.org/web/packages/statcanR/vignettes/statCanR.html) does but for Python
and offers Pandas or Polars dataframes, as required.

## Installation 

This is not yet published on PyPI so meanwhile install it from the repo.

```shell
pip install git+https://github.com/aalekhpatel07/statcan
```

## Usage

Either via the `statcan` executable:

```
usage: statcan [-h] [-v] [--polars] [-n RETURN_ROWS] {search,download} ...

Download wrangled datasets for Pandas or Polars from StatCAN just like how https://github.com/warint/statcanR does it in R.

options:
  -h, --help            show this help message and exit
  -v, --verbose         increase verbosity (default: 0)
  --polars              Use polars instead of pandas. Note: This requires 'polars' extra be installed. (default: False)
  -n RETURN_ROWS, --return-rows RETURN_ROWS
                        Number of rows to return (default is whatever df.head() returns) (default: None)

command:
  {search,download}     The two main ways of consuming the StatCAN datasets.
```

or via the library that powers the CLI:

```python

from pathlib import Path
from statcan.client import StatCan, MetadataDatabase, Language



# To search for datasets containing keywords:
db = MetadataDatabase()
db.load()
df = db.search("labour", "force")
print(df.head())


# To download the cleaned dataset corresponding to a given table number.
client = StatCan()

# Get the table_number by running the search. 
# For example:
table_number = "34-10-0281-01"

language = Language.ENGLISH
save_dir = Path(".")  # save the downloaded and cleaned csv to current dir.

csv = client.download(table_number, language, save_dir=save_dir)
df = csv.get_df_pandas()
print(df.head())

```