"""Downloads NYC Green Taxi Parquet files into /src/raw"""

from prefect import task
from urllib.request import urlretrieve


@task(retries=3, retry_delay_seconds=10, log_prints=True)
def download(file_path):
    file_path.mkdir(parents=True, exist_ok=True)
    
    for month in range(1,4):
        file_name = f"green_tripdata_2025-{month:02d}.parquet"
        url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/{file_name}"
        file_dir = file_path / file_name

        urlretrieve(url, file_dir)
        print(f"Saved in path: {file_dir}")

#'C:\Users\moma0\Documents\Weiterbildung\00_GitHub_Repo\Modul2\mle-data-pipeline-project\data\raw'
#January - 2025: https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2025-01.parquet
#February - 2025: https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2025-02.parquet
#March - 2025: https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2025-03.parquet