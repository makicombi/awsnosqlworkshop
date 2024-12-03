#!/usr/bin/env python3
import os, sys
import json
import boto3
from botocore.exceptions import ClientError
from datetime import datetime

genstring = round(datetime.now().timestamp())

C_AVAILABLE = 'AVAILABLE'
C_INUSE = 'IN_USE'
                  
insert_items=[
  {
  "PK": {
    "S": "ASSET#B74819"
  },
  "SK": {
    "S": f"SERVICE#SVC{genstring}C"
  },
  "AssetID": {
    "S": "B74819"
  },
  "ServiceCreateDate": {
    "S": "2023-05-14T16:55:02.698222"
  },
  "ServiceDate": {
    "S": "2023-05-26T16:55:02.698222"
  },
  "ServiceID": {
    "S": f"SVC{genstring}C"
  },
  "ServiceNotes": {
    "S": "--- GxseCcUmmCruQZxalMeVquZbxHxAclgEawQdWDGXmjouulqvdnLeaBGVdRLBdmsjZvMtAWpRCfNZiwaPFQasOeyZDfKGVxcTeNyxCFBsUsrCFAEUzsLDNldBfJAJeIljiBLFmHEGcKm"
  },
  "ServiceStatus": {
    "S": "COMPLETED"
  }
},
  {
  "PK": {
    "S": "ASSET#B74819"
  },
  "SK": {
    "S":  f"SERVICE#SVC{genstring}O"
  },
  "AssetID": {
    "S": "B74819"
  },
  "ServiceCreateDate": {
    "S": "2024-05-14T16:55:02.698222"
  },
  "ServiceID": {
    "S": f"SVC{genstring}O"
  },
  "ServiceNotes": {
    "S": "--- !! GxseCcUmmCruQZxalMeVquZbxHxAclgEawQdWDGXmjouulqvdnLeaBGVdRLBdmsjZvMtAWpRCfNZiwaPFQasOeyZDfKGVxcTeNyxCFBsUsrCFAEUzsLDNldBfJAJeIljiBLFmHEGcKm"
  },
  "ServiceStatus": {
    "S": "OPEN"
  },
  "GSI1_PK": {
    "S": "OPEN#B74819"
  }
},
]    


assets_in_use = ['ASSET#B74819','ASSET#S76328','ASSET#B17517'] 
assests_avlbl = ['ASSET#S53739','ASSET#B26055','ASSET#S08312','ASSET#B48879']

items = []

table = boto3.resource('dynamodb').Table('fleet')

boto_args = {'service_name': 'dynamodb'}
dynamodb_client = boto3.client("dynamodb")


def gen_bike_events(run):
  
  asset_cnt = 0
  service_cnt = 0
  
  
  #flip status from AVAILABLE to IN_USE or vice-versa 
  for r in assets_in_use:
    asset_cnt += 1
    status = C_AVAILABLE
    if (run == '1'):
      status = C_INUSE
    
    upd_item_request = {
        "TableName": "fleet",
        "Key": { 
              "PK": {
                "S": r
              },
              "SK": {
                "S": r
              }
              },
        "UpdateExpression": "SET #status = :status",
        "ExpressionAttributeNames": { '#status': 'Status'},
        "ExpressionAttributeValues": {':status': { 'S': status } }
        }
    
    dynamodb_client.update_item(**upd_item_request)
    
  for r in assests_avlbl:
    asset_cnt += 1
    status = C_INUSE
    if (run == '1'):
      status = C_AVAILABLE
    
    upd_item_request = {
      "TableName": "fleet",
      "Key": { 
            "PK": {
              "S": r
            },
            "SK": {
              "S": r
            }
            },
      "UpdateExpression": "SET #status = :status",
      "ExpressionAttributeNames": { '#status':  'Status' },
      "ExpressionAttributeValues": {':status': { 'S': status } }
      }
  
    dynamodb_client.update_item(**upd_item_request)
  
  a = dict({ "PK":{"S": f"ASSET#B{genstring}"},"SK":{"S": f"ASSET#B{genstring}"},
                      "AssetID":{"S": f"B{genstring}"},"AssetType":{"S":"EBIKE"},"Battery":{"N":"90"},
                      "Latitude":{"S":"38.88752942448752"},"Longitude":{"S":"-76.9567453111383"},"Status":{"S":C_AVAILABLE}
                      })

  
  put_item_request = { 'TableName': "fleet", 'Item': a }
  dynamodb_client.put_item(**put_item_request)
  asset_cnt += 1
  
   
  for i in insert_items: 
    service_cnt += 1
    put_item_request = { 'TableName': "fleet", 'Item': i }
    dynamodb_client.put_item(**put_item_request)
    
  
  print(f'Bike events generated| Total: {asset_cnt + service_cnt} | Asset items: {asset_cnt} | Service items: {service_cnt}')
      
      
if __name__ == '__main__':
    globals()[sys.argv[1]](sys.argv[2])