import requests
import json
from datetime import datetime
from pathlib import Path

URL = "https://opensky-network.org/api/states/all"


def fetch_flight_data():

    response = requests.get(URL, timeout=30)
    response.raise_for_status()

    data = response.json()

    timestamp = datetime.utcnow().strftime("%Y%m%d%H%M%S")

    bronze_dir = Path("/opt/airflow/data/bronze")
    bronze_dir.mkdir(parents=True, exist_ok=True)

    file_path = bronze_dir / f"flight_data_{timestamp}.json"

    with open(file_path, "w") as f:
        json.dump(data, f)

    print(f"Flight data saved to {file_path}")

    return str(file_path)