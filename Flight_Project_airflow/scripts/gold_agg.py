import json
import pandas as pd
from pathlib import Path


def transform_bronze_to_silver(bronze_file):

    print(f"Reading bronze file: {bronze_file}")

    with open(bronze_file, "r") as f:
        data = json.load(f)

    df = pd.DataFrame(data["states"])

    df.columns = [
        "icao24","callsign","origin_country","time_position","last_contact",
        "longitude","latitude","baro_altitude","on_ground","velocity",
        "true_track","vertical_rate","sensors","geo_altitude",
        "squawk","spi","position_source"
    ]

    silver_dir = Path("/opt/airflow/data/silver")
    silver_dir.mkdir(parents=True, exist_ok=True)

    silver_file = silver_dir / "flights_cleaned.parquet"

    df.to_parquet(silver_file)

    print(f"Silver data saved to {silver_file}")

    # IMPORTANT
    return str(silver_file)