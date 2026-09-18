import pandas as pd
from prefect import task


@task(log_prints=True)
def transform_data(data):
    # Nur die benötigten Spalten kopieren
    rides = data[
        ["lpep_pickup_datetime", "total_amount"]
    ].copy()

    # Abholzeitpunkt in einen Datums-/Zeitwert umwandeln
    rides["lpep_pickup_datetime"] = pd.to_datetime(
        rides["lpep_pickup_datetime"],
        errors="raise",
    )

    if rides.isna().any().any():
        raise ValueError("'lpep_pickup_datetime' or 'total_amount' is missing.")

    # Uhrzeit entfernen, Fahrten desselben Tages zusammenfassen
    rides["ride_date"] = rides["lpep_pickup_datetime"].dt.date

    daily_revenue = (
        rides.groupby("ride_date", as_index=False)
        .agg(
            total_revenue=("total_amount", "sum"),
            ride_count=("total_amount", "size"),
        )
        .sort_values("ride_date")
    )

    daily_revenue["total_revenue"] = (
        daily_revenue["total_revenue"].round(2)
    )

    return daily_revenue