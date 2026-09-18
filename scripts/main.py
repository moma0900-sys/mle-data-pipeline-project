"""Main"""

from prefect import flow

from download_raw import download
from merge_raw_parquet import merge_files
from check_data_raw import inspect_data
from paths import RAW_DIR
from clean_data import clean_data
from transform_data import transform_data
from load_cleaned_data import load_data_parquet


@flow(name="Green Taxi ETL", log_prints=True)
def main():
    print("1. Downloading Data")
    download(RAW_DIR)

    print("2. Reading and merge Data")
    merged_data = merge_files()

    print("3. Checking Data")
    inspect_data(merged_data)

    cleaned_data = clean_data(merged_data)
    daily_revenue = transform_data(cleaned_data)

    load_data_parquet(daily_revenue)
    print(f"Daily Revenue: \n {daily_revenue.head()}")


if __name__ == "__main__":
    main()