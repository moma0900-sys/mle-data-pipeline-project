"""
Daten prüfen:
 1. Spalten
 2. Datentypen
 3. fehlende Werte
 4. Datumsbereich ansehen
"""

from prefect import task


@task(log_prints=True)
def inspect_data(data):
    print(data.head(),"\n\n")

    print (data.info(show_counts=True))
    print("\n")

    print("Empty Dataframes:\n", data.isna().sum(),"\n\n")

    print("First Pick-Up:", data["lpep_pickup_datetime"].min(),"\n\n")
    print("Last Pick-Up:", data["lpep_pickup_datetime"].max(),"\n\n")

    print(data["total_amount"].describe())