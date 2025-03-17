import json
import requests
import pandas as pd
import os
import boto3


def get_station_data():

  # aws credentials
  os.environ['AWS_PROFILE'] = "MyProfile1"
  os.environ['AWS_DEFAULT_REGION'] = "us-east-1"

  #get station json
  wifi_stations_response = requests.get("https://data.ny.gov/resource/pwa9-tmie.json")
  data = wifi_stations_response.text
  parse_wifi_json = json.loads(data)



  #parse station json data
  wifi_stations_list = []

  for response in parse_wifi_json:
    item = {
      "station_name": response.get("station_name"),
      "station_complex": response.get("station_complex"),
      "borough": response.get("borough"),
      "is_historical": response.get("historical"),
      "wifi_available": response.get("wifi_available"),
      "att":response.get("at_t"),
      "sprint":response.get("sprint"),
      "verizon": response.get("verizon"),
      "tmobile": response.get("t_mobile")
    }
    wifi_stations_list.append(item)

  station_df = pd.DataFrame(wifi_stations_list)


  #clean wifi data
  station_df = pd.eval("wifi_available = (station_df.wifi_available == 'Yes')", target=station_df)

  station_df = pd.eval("service_available = (station_df.att == 'Yes') or (station_df.sprint == 'Yes') or (station_df.verizon == 'Yes') or (station_df.tmobile == 'Yes')", target=station_df)

  station_df = station_df.drop(columns = ["att", "sprint", "verizon", "tmobile"])

  print(station_df.head())

  #save to AWS
  station_df.to_csv("s3://kate-mta-airflow-bucket/cleaned_data/station_data.csv")


