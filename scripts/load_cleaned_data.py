from paths import PROCESSED_DIR
from prefect import task


@task(log_prints=True)
def load_data_parquet(data):
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

    output_file = PROCESSED_DIR / "daily_revenue.parquet"
    data.to_parquet(output_file, index=False)

    print(f"Saving Results to: {output_file}")
    return output_file