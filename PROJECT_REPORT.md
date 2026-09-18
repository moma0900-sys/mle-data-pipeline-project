# Project Report: NYC Green Taxi Data Pipeline

## 1. Project Goal

The goal of this project was to build a data pipeline that processes NYC Green Taxi trip data for January, February, and March 2025 and calculates revenue per pickup day.

The implementation follows an **ETL (Extract, Transform, Load)** approach:

1. Download and read the source files.
2. Inspect, clean, and aggregate the data in Python.
3. Save the daily results as a Parquet file.

The optional bonus task was also implemented: **Prefect orchestrates the workflow and tracks the individual processing steps.**

## 2. Tools Used

- **Python** for the pipeline implementation
- **pandas** for data inspection, cleaning, and aggregation
- **PyArrow** for Parquet support
- **Prefect** for workflow orchestration, logging, and retries
- **uv** for dependency management and execution
- **Git and GitHub** for version control and collaboration

## 3. Project Structure

```text
mle-data-pipeline-project/
├── README.md
├── PROJECT_REPORT.md
├── pyproject.toml
├── uv.lock
├── scripts/
│   ├── paths.py
│   ├── download_raw.py
│   ├── merge_raw_parquet.py
│   ├── check_data_raw.py
│   ├── clean_data.py
│   ├── transform_data.py
│   ├── load_cleaned_data.py
│   └── main.py
└── data/
    ├── raw/
    │   ├── green_tripdata_2025-01.parquet
    │   ├── green_tripdata_2025-02.parquet
    │   └── green_tripdata_2025-03.parquet
    └── processed/
        └── daily_revenue.parquet
```

The data directories are created automatically when needed.

## 4. Running the Pipeline

Run all commands from the project root.

### Install dependencies

```powershell
uv sync
```

### Start the Prefect server

In the first terminal:

```powershell
uv run prefect server start
```

Keep this terminal open while running the pipeline.

### Configure the API connection and run the pipeline

In a second terminal:

```powershell
uv run prefect config set PREFECT_API_URL=http://127.0.0.1:4200/api
uv run python scripts/main.py
```

The API configuration is saved in the active Prefect profile. It does not need to be repeated for every run unless the configuration changes.

### View the workflow

Open the Prefect dashboard:

http://127.0.0.1:4200

The workflow appears under the name **Green Taxi ETL**.

The pipeline requires an internet connection for downloading the source files. Each execution currently downloads all three files again and overwrites existing files with the same names.

## 5. Pipeline Stages

`main.py` controls the execution order and passes results between the processing functions.

### Download

**Module:** `download_raw.py`

Downloads the Green Taxi Parquet files for January, February, and March 2025 into `data/raw`.

The destination directory is created if it does not exist.

### Extract

**Module:** `merge_raw_parquet.py`

Reads all `.parquet` files directly inside `data/raw` and combines them into one pandas DataFrame.

File discovery happens inside the function, after the download step. This allows the pipeline to start even when no source files have been downloaded yet.

For this project, the directory contains the three monthly source files.

### Inspect

**Module:** `check_data_raw.py`

Displays:

- Sample rows
- Column names and data types
- Missing values per column
- Earliest and latest pickup timestamps
- Descriptive statistics for `total_amount`

This step supports manual inspection. It does not automatically enforce data-quality rules.

### Clean

**Module:** `clean_data.py`

Removes rows where either of these required fields is missing:

- `lpep_pickup_datetime`
- `total_amount`

Missing values in other columns do not cause a row to be removed because those columns are not required for the daily revenue calculation.

The function reports the number of rows before cleaning, the number removed, and the number remaining.

### Transform

**Module:** `transform_data.py`

The transformation:

1. Selects the pickup timestamp and total amount.
2. Converts pickup timestamps to datetime values.
3. Checks that the required values are present.
4. Extracts the pickup date.
5. Groups rides by date.
6. Calculates total revenue and the number of rides per day.
7. Rounds daily revenue to two decimal places.
8. Sorts the result by date.

### Load

**Module:** `load_cleaned_data.py`

Saves the aggregated results to:

```text
data/processed/daily_revenue.parquet
```

The output directory is created automatically. The DataFrame index is not written to the file.

Each successful run overwrites the previous output.

## 6. Revenue Definition and Data Rules

For this project, **revenue is defined as the sum of `total_amount`**, rather than the base fare alone.

Each ride is assigned to its pickup date, using `lpep_pickup_datetime`.

The current implementation:

- Removes rows with missing pickup timestamps or total amounts
- Retains zero and negative amounts
- Does not remove duplicate rows
- Does not filter pickup dates to January–March 2025
- Stops if a non-missing pickup timestamp cannot be converted to datetime

Although the source files cover January–March 2025, individual records may contain pickup dates outside that period. These records are currently included.

Negative amounts are retained without assuming that they are errors. A production pipeline would require an explicit business rule for handling corrections or refunds.

## 7. Output

The output contains one row per pickup date present in the cleaned data.

| Column | Meaning |
|---|---|
| `ride_date` | Pickup date |
| `total_revenue` | Sum of total amounts for that date |
| `ride_count` | Number of included rides for that date |

Dates without rides are not added automatically.

The raw monthly files and the aggregated Parquet output have been generated locally.

## 8. Prefect Orchestration

The pipeline uses one Prefect flow and six tasks.

### Flow

The `main()` function is decorated with:

```python
@flow(name="Green Taxi ETL", log_prints=True)
```

It coordinates the complete workflow.

### Tasks

The following functions are decorated with `@task`:

1. `download`
2. `merge_files`
3. `inspect_data`
4. `clean_data`
5. `transform_data`
6. `load_data_parquet`

The tasks are called directly, so each finishes before the next step starts.

### Logging

The flow and tasks use `log_prints=True`, which sends their `print()` output to Prefect logs.

This makes progress messages and processing information available in the dashboard.

### Retries

The download task uses:

```python
@task(
    retries=3,
    retry_delay_seconds=10,
    log_prints=True,
)
```

If the task fails, Prefect can retry it up to three times, waiting ten seconds between attempts.

A retry repeats the complete download task, including its loop over the three monthly files.

### Execution

The workflow is started manually from the command line. No scheduled deployment or separate worker is configured.

The Prefect-enabled pipeline was run successfully.

## 9. Challenges and Lessons Learned

### File Paths

I learned to define shared paths centrally in `paths.py`. This avoids repeating directory definitions across scripts and keeps paths independent of the terminal's working directory.

### Python Indentation

Initially, the download command was outside the loop, so only the final month's file was downloaded. Moving it inside the loop ensured that each monthly file was downloaded.

### Functions and Return Values

I learned the difference between referencing a function and calling it.

For example, `merge_files` refers to the function, while `merge_files()` executes it and returns a DataFrame.

I also learned to pass returned DataFrames between the pipeline stages.

### Imports and Execution Order

File discovery originally happened when the merge module was imported. This prevented the pipeline from starting when no raw files existed yet.

Moving file discovery into `merge_files()` allows the download step to run first.

### Separating Responsibilities

Separate functions for downloading, reading, inspecting, cleaning, transforming, and saving made the pipeline easier to understand and maintain.

The central `main()` function defines the overall execution order.

### Prefect Integration

Adding Prefect decorators allowed me to monitor the existing processing functions as tasks without rewriting their core logic.

I learned how to connect the workflow to a local Prefect server and configure retries for the download step.

## 10. Improvements with More Time

I would:

- Add automated tests for cleaning and daily aggregation.
- Compare output totals and ride counts with the cleaned input.
- Validate required columns and data types before processing.
- Define explicit rules for out-of-period dates, duplicates, and negative amounts.
- Skip downloads when valid source files already exist.
- Download to temporary files before replacing existing source files.
- Make the year, months, and output location configurable.
- Add a scheduled Prefect deployment.
- Extend the load step to support a database such as PostgreSQL.

## 11. Project Status

The required functionality is implemented:

- Download the first three months of 2025.
- Read the locally staged Parquet files.
- Inspect and clean the data.
- Calculate revenue per day.
- Save the results as a Parquet file.

The optional Prefect orchestration task is also implemented, and the pipeline has been run successfully.