import json
import requests
import pandas as pd
import os
import boto3


def get_rider_data():
  print("hello I am running in mta etl 2")

  # aws credentials
  os.environ['AWS_PROFILE'] = "MyProfile1"
  os.environ['AWS_DEFAULT_REGION'] = "us-east-1"


  #get rider data
  rider_data_response = requests.get("https://data.ny.gov/resource/wujg-7c2s.json")
  data = rider_data_response.text
  parse_rider_json = json.loads(data)

  rider_data_list = []

  for response in parse_rider_json:
    item = {
      "transit_date": response.get("transit_timestamp"),
      "station_complex": response.get("station_complex"),
      "transit_mode": response.get("transit_mode"),
      "payment_method": response.get("payment_method"),
      "fare_class_category": response.get("fare_class_category"),
      "transfers": response.get("transfers")
    }
    rider_data_list.append(item)

  rider_df = pd.DataFrame(rider_data_list)


  #clean rider data
  rider_df = rider_df.astype({"transit_date": "datetime64[s]"})
  rider_df = rider_df.astype({"transfers":"float"})
  rider_df = rider_df.astype({"transfers":"int"})
  rider_df = rider_df.astype({"station_complex":"string"})
  rider_df = rider_df.astype({"transit_mode":"string"})
  rider_df = rider_df.astype({"payment_method":"string"})
  rider_df = rider_df.astype({"fare_class_category":"string"})


  #save clean rider data to S3
  rider_df.to_csv("s3://kate-mta-airflow-bucket/cleaned_data/rider_data.csv")
