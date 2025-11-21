import pandas as pd
import requests
from datetime import datetime
import json
import glob 
import os
import openmeteo_requests
import pandas as pd
import requests_cache
from retry_requests import retry
import time

cache_session = requests_cache.CachedSession('.cache', expire_after = -1)
retry_session = retry(cache_session, retries = 5, backoff_factor = 0.2)
openmeteo = openmeteo_requests.Client(session = retry_session)

count_limit = 51

column_names = ["temperature_2m", "relative_humidity_2m", "apparent_temperature", "precipitation", "rain", "snowfall", 
                "weather_code", "cloud_cover", "cloud_cover_low", "cloud_cover_mid", "cloud_cover_high"]

path_sessions_dir = "../data/Sessions"

def read_session_files(path_sessions_dir: str):
	all_files = glob.glob(os.path.join(path_sessions_dir , "*.csv"))
	print(len(all_files))
	print(all_files)
	return all_files


def get_weather(lat, lon, date_time):

    """
    Query Open-Meteo historical weather API for a given location and datetime.
    Returns cloud cover, temperature, precipitation, etc.
    """
    # Format datetime to API requirement
    dt = datetime.strptime(date_time, "%Y-%m-%d %H:%M:%S")
    date_str = dt.strftime("%Y-%m-%d")
    hour_str = dt.strftime("%H") + ":00"

    url = "https://archive-api.open-meteo.com/v1/archive"
    params = {
        "latitude": lat,
        "longitude": lon,
        "start_date": date_str,
        "end_date": date_str,
        "hourly": column_names,
    }


    responses = openmeteo.weather_api(url, params=params)
    response = responses[0]
# Process hourly data. The order of variables needs to be the same as requested.
    hourly = response.Hourly()
    hourly_temperature_2m = hourly.Variables(0).ValuesAsNumpy()
    hourly_relative_humidity_2m = hourly.Variables(1).ValuesAsNumpy()
    hourly_apparent_temperature = hourly.Variables(2).ValuesAsNumpy()
    hourly_precipitation = hourly.Variables(3).ValuesAsNumpy()
    hourly_rain = hourly.Variables(4).ValuesAsNumpy()
    hourly_snowfall = hourly.Variables(5).ValuesAsNumpy()
    hourly_weather_code = hourly.Variables(6).ValuesAsNumpy()
    hourly_cloud_cover = hourly.Variables(7).ValuesAsNumpy()
    hourly_cloud_cover_low = hourly.Variables(8).ValuesAsNumpy()
    hourly_cloud_cover_mid = hourly.Variables(9).ValuesAsNumpy()
    hourly_cloud_cover_high = hourly.Variables(10).ValuesAsNumpy()
    hourly_data = {"date": pd.date_range(
        start = pd.to_datetime(hourly.Time(), unit = "s", utc = True),
        end = pd.to_datetime(hourly.TimeEnd(), unit = "s", utc = True),
        freq = pd.Timedelta(seconds = hourly.Interval()),
        inclusive = "left"
    )}

    hourly_data["temperature_2m"] = hourly_temperature_2m
    hourly_data["relative_humidity_2m"] = hourly_relative_humidity_2m
    hourly_data["apparent_temperature"] = hourly_apparent_temperature
    hourly_data["precipitation"] = hourly_precipitation
    hourly_data["rain"] = hourly_rain
    hourly_data["snowfall"] = hourly_snowfall
    hourly_data["weather_code"] = hourly_weather_code
    hourly_data["cloud_cover"] = hourly_cloud_cover
    hourly_data["cloud_cover_low"] = hourly_cloud_cover_low
    hourly_data["cloud_cover_mid"] = hourly_cloud_cover_mid
    hourly_data["cloud_cover_high"] = hourly_cloud_cover_high
    #print(hourly_data)

    # Find the closest hour index
    hours = hourly_data["date"].astype(str).tolist()
    
    #print("hours: ", hours)
    #print(hours)
    if hour_str not in [t[-14:-9] for t in hours]:
        print(f"Hour {hour_str} not found in data for date {date_str}. Available hours: {[t[-14:-9] for t in hours]}")
        return None

    idx = [t[-14:-9] for t in hours].index(hour_str)
    #print(idx)

    data_return = {}
    for attribute in column_names:
        data_return[attribute] = str(hourly_data[attribute][idx])

    #print(data_return)
    return data_return


def compute_weather_data(all_files: list):
	global count_limit
	index = 0
	for file in all_files:
		# Resets cache every file to avoid memory issues
		if index <= 3:
			index += 1
			continue
		cache_session.cache.clear()
		weather_data = []
		df = pd.read_csv(file, index_col=None, header=0, sep=';')
		for i, row in df.iterrows():
			temp = [row["Session ID"]]
			print(f"Getting weather for Session ID {row['Session ID']} at lat={row['Latitude']}, lon={row['Longitude']}, datetime={row['Start Date']}")

			# If the values are null, skip
			if pd.isna(row["Latitude"]) or pd.isna(row["Longitude"]):
				continue

			weather = get_weather(row["Latitude"], row["Longitude"], row["Start Date"].replace('"',''))
			for key, value in weather.items():
				temp.append(value)
			weather_data.append(temp)
			count_limit += 1
			if count_limit >= (3000):  # Adjusted for estimated number of calls per request
				print("Reached 5000 API calls, sleeping for 1 hour to avoid rate limiting...")
				time.sleep(3600)
				count_limit = 0
				
		df_weather = pd.DataFrame(weather_data, columns=["Session ID"] + column_names)
		df_weather.to_csv(os.path.join("../data/Weather", os.path.basename(file)), index=False)

		index += 1

if __name__ == "__main__":
	all_files = read_session_files(path_sessions_dir)
	compute_weather_data(all_files)