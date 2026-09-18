"""Erase rows with empty values for lpep_pickup_datetime and total_amount"""

from prefect import task


@task(log_prints=True)
def clean_data(data):
    cleaned = data.dropna(
        subset=["lpep_pickup_datetime", "total_amount"]
    ).copy()

    removed = len(data) - len(cleaned)

    print(f"Rows before cleaning: {len(data)}")
    print(f"Removed Rows: {removed}")
    print(f"Rows after cleaning: {len(cleaned)}")

    return(cleaned)