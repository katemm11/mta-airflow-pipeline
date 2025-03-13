import json
import requests
import pandas as pd
import os
import boto3

print("hello I am running before the function")

def run_mta_etl():

  print("hello I am running")

  #get station data
  wifi_stations_response = requests.get("https://data.ny.gov/resource/pwa9-tmie.json")
  data = wifi_stations_response.text
  parse_wifi_json = json.loads(data)


  #get rider data
  rider_data_response = requests.get("https://data.ny.gov/resource/wujg-7c2s.json")
  data = rider_data_response.text
  parse_rider_json = json.loads(data)

  # #store raw datasets to s3
  os.environ['AWS_PROFILE'] = "MyProfile1"
  os.environ['AWS_DEFAULT_REGION'] = "us-east-1"

  # s3 = boto3.resource('s3', region_name='us-east-1')
  # s3object = s3.Object('kate-mta-aws-bucket', 'raw_wifi_data.json')

  # s3object.put(
  #   Body=(bytes(json.dumps(parse_wifi_json).encode('UTF-8')))
  # )

  # s3object = s3.Object('kate-mta-aws-bucket', 'raw_rider_data.json')
  # s3object.put(
  #   Body=(bytes(json.dumps(parse_rider_json).encode('UTF-8')))
  # )

  #create dataframes
  wifi_stations_list = []

  for response in parse_wifi_json:
    item = {
      "station_complex": response.get("station_complex"),
      "att":response.get("at_t"),
      "sprint":response.get("sprint"),
      "verizon": response.get("verizon"),
      "tmobile": response.get("t_mobile"),
      "historical": response.get("historical")
    }
    wifi_stations_list.append(item)

  wifi_df = pd.DataFrame(wifi_stations_list)


  rider_data_list = []

  for response in parse_rider_json:
    item = {
      "transit_date": response.get("transit_timestamp"),
      "station_complex": response.get("station_complex")
    }
    rider_data_list.append(item)

  rider_df = pd.DataFrame(rider_data_list)
  rider_df = rider_df.astype({"transit_date": "datetime64[s]"})
  rider_df = rider_df.astype({"transit_date": "date32[pyarrow]"})


  #aggregate data

  combined_df = rider_df.merge(wifi_df, left_on="station_complex", right_on="station_complex")


  combined_df = pd.eval("provider_available = (combined_df.att == 'Yes') or (combined_df.sprint == 'Yes') or (combined_df.verizon == 'Yes') or (combined_df.tmobile == 'Yes')", target=combined_df)

  combined_df = combined_df.drop(columns = ["station_complex","att", "sprint", "verizon", "tmobile"])
  print(combined_df)
  combined_df.to_csv("s3://kate-mta-airflow-bucket/mta_data.csv")
