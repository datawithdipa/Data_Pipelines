import requests
import pandas as pd
from datetime import datetime, timedelta
import os

def process_earthquake_data():
    today_date = datetime.now()
    # Date 15 days ago
    date_15_days_ago = today_date - timedelta(days=15)

    start_time = date_15_days_ago.strftime('%Y-%m-%d')
    end_time = today_date.strftime('%Y-%m-%d')

    # Define the API endpoint
    url = f"https://earthquake.usgs.gov/fdsnws/event/1/query?format=geojson&starttime={start_time}&endtime={end_time}"

    # Send a GET request to the API endpoint
    response = requests.get(url)

    # Check if the request was successful
    if response.status_code == 200:
        # Parse the response content as JSON
        data = response.json()

        # Extract earthquake features
        features = data['features']

        # Create a list of dictionaries containing relevant data
        earth_quakes = []
        date = today_date.strftime("%Y_%m_%d")
        filename = f"/Users/Documents/basic_pipeline/data/earthquake_{date}.csv"

        for feature in features:
            properties = feature['properties']
            geometry = feature['geometry']
            earthquake = {
                'time': properties['time'],
                'place': properties['place'],
                'magnitude': properties['mag'],
                'longitude': geometry['coordinates'][0],
                'latitude': geometry['coordinates'][1],
                'depth': geometry['coordinates'][2],
                'file_name': filename
            }
            earth_quakes.append(earthquake)

        # Convert the list of dictionaries to a DataFrame
        df = pd.DataFrame(earth_quakes)

        # Check if the file exists
        if os.path.exists(filename):
            # If it exists, remove it
            os.remove(filename)
            print(f"File {filename} removed.")

        # Now, create a new file
        df.to_csv(filename, index=False)
        print(f"File {filename} created and written to.")

    else:
        print(f"Failed to retrieve data: {response.status_code}")


def main():
    process_earthquake_data()


if __name__ == "__main__":
    main()
