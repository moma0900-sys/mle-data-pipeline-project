"""Merge the three files to one table"""

import pandas as pd
from paths import RAW_DIR
from prefect import task


@task(log_prints=True)
def merge_files():
    files = sorted(RAW_DIR.glob("*.parquet"))
    if not files:
        raise FileNotFoundError(f"No Parquet-File found in {RAW_DIR}.")
    
    dataframe = [pd.read_parquet(file) for file in files]
    merge = pd.concat(dataframe, ignore_index=True)

    return merge