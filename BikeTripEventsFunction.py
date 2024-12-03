import os
import json
from boto3.dynamodb.types import TypeDeserializer

C_STATUS_AVAILABLE = 'AVAILBLE'
C_STATUS_INUSE = 'IN_USE'
C_Status = 'Status'

# Environment settings
TABLE = os.environ.get('TABLE','fleet')
REGION = os.environ.get('AWS_REGION')

def dynamo_obj_to_python_obj(dynamo_obj: dict) -> dict:
    deserializer = TypeDeserializer()
    return {
        k: deserializer.deserialize(v) 
        for k, v in dynamo_obj.items()
    } 


def process_bike_events(record):
    '''
    process fleet stream events - Detect asset's 'Status' changes to AVAILABLE or IN_USE
    Params:
        record - Stream event JSON as python dictionary
    '''
    
    ## Depending on the type of event - INSERT, REMOVE or MODIFY - Get New and Old Images of the item
    if record['dynamodb'].get('NewImage',None):
        new_image = dynamo_obj_to_python_obj(record['dynamodb']['NewImage'])
    if record['dynamodb'].get('OldImage',None):
        old_image = dynamo_obj_to_python_obj(record['dynamodb']['OldImage'])
    
    ## Get the item keys
    keys = dynamo_obj_to_python_obj(record['dynamodb']['Keys'])
    
    ## Process only if all of the below conditions are met
    ## 1. Asset items (excluding service records)
    ## 2. INSERT event: Status is in 'AVAILABLE' or 'IN_USE'
    ## 3. UPDATE event: Status changed from 'AVAILABLE' or 'IN_USE'
    if str(keys['PK']).startswith('ASSET#') and  str(keys['SK']).startswith('ASSET#'):
        if (
                record['eventName'] == "INSERT" and new_image['Status'] in ['AVAILABLE','IN_USE']
           or
               ( record['eventName'] == "MODIFY" 
                and new_image['Status'] != old_image['Status'] 
                and new_image['Status'] in ['AVAILABLE','IN_USE'] 
               )
           ):
                ''' Get asset's attributes: Latitude, Longitude and Battery '''
                print(f"AssetID: {keys['PK']} | Geo-coordinates: [{new_image['Latitude']}.{new_image['Longitude']}] | Battery: {new_image['Battery']} | Status: {new_image['Status']}")
                
        if record['eventName'] == 'REMOVE':
            
            ''' Get asset's attributes: Latitude, Longitude and Battery '''
            print(f"AssetID: {keys['PK']} | Geo-coordinates: [{old_image['Latitude']}.{old_image['Longitude']}] | Battery: {old_image['Battery']} | Status: {old_image['Status']}")
                
    return

def lambda_handler(event, context):
    #print("Received event: " + json.dumps(event, indent=2))
    for record in event['Records']:
        print(record['eventName'])
        print("DynamoDB Record: \n" + json.dumps(record['dynamodb']))
        try:
            process_bike_events(record)
        except:
            print(f'Unable to process trip: {json.dumps(record)}')
            raise
    print('Successfully processed {} records.'.format(len(event['Records'])))