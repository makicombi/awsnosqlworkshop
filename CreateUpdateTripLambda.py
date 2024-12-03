
# Load the AWS SDK for Python
import json
import boto3
from boto3.dynamodb.conditions import Key, Attr
import os
from botocore.exceptions import ClientError
import time
from decimal import *
import random
from datetime import datetime

ERROR_HELP_STRINGS = {
    # Operation specific errors
    'ConditionalCheckFailedException': 'Condition check specified in the operation failed, review and update the condition check before retrying',
    'TransactionConflictException': 'Operation was rejected because there is an ongoing transaction for the item, generally safe to retry with exponential back-off',
    'ItemCollectionSizeLimitExceededException': 'An item collection is too large, you\'re using Local Secondary Index and exceeded size limit of items per partition key.' +
                                                ' Consider using Global Secondary Index instead',
    # Common Errors
    'InternalServerError': 'Internal Server Error, generally safe to retry with exponential back-off',
    'ProvisionedThroughputExceededException': 'Request rate is too high. If you\'re using a custom retry strategy make sure to retry with exponential back-off.' +
                                              'Otherwise consider reducing frequency of requests or increasing provisioned capacity for your table or secondary index',
    'ResourceNotFoundException': 'One of the tables was not found, verify table exists before retrying',
    'ServiceUnavailable': 'Had trouble reaching DynamoDB. generally safe to retry with exponential back-off',
    'ThrottlingException': 'Request denied due to throttling, generally safe to retry with exponential back-off',
    'UnrecognizedClientException': 'The request signature is incorrect most likely due to an invalid AWS access key ID or secret key, fix before retrying',
    'ValidationException': 'The input fails to satisfy the constraints specified by DynamoDB, fix input before retrying',
    'RequestLimitExceeded': 'Throughput exceeds the current throughput limit for your account, increase account level throughput before retrying',
}


def create_dynamodb_client():  # Instigate a dynamodb client for talking to tables. 
    region=boto3.session.Session().region_name
    return boto3.client("dynamodb", region_name=region)


def create_new_trip_item(userid,tripsk,tripid,tripstart): #this creates a dict in a format that the boto3 dyanmodb client likes when creating items, using input from the POST body
    return {
        "PK": {"S": userid},
        "SK": {"S": tripsk},
        "TripID": {"S": tripid},
        "StartTs": {"S": tripstart}
    }

def execute_create_trip_item(dynamodb_client, input): # This takes the input item from create_new_trip_item, and passes it as the new Item for a put_item call to the trips table
    try:
        response = dynamodb_client.put_item(
        TableName='trips',
        Item=input
        )
        print("Trip created")
    except ClientError as error:
        handle_error(error)
        raise
    except BaseException as error:
        print("Unknown error while updating item: ")
        raise


def update_end_of_trip_trip_item(userid,tripsk,endTs,miles,fare):  #this creates a dict in a format that the boto3 dynamodb client likes when updating items, using input from the PUT body
    return {
        "TableName": "trips",
        "Key": {
            "PK": {"S": userid},
            "SK": {"S": tripsk}
        },
        "UpdateExpression": "SET #b7540 = :b7540, #b7541 = :b7541, #b7542 = :b7542",
        "ExpressionAttributeNames": {"#b7540":"EndTs","#b7541":"Miles","#b7542":"Fare"},
        "ExpressionAttributeValues": {":b7540": {"S":endTs},":b7541": {"N": str(miles)},":b7542": {"N": str(fare)}}
    }

def update_bike_item(assetid,status): #this creates a dict in a format that the boto3 dynamodb client likes when updating items, using input from the POST or PUT body to identify the asset
    return {
        "TableName": "fleet",
        "Key": {
            "PK": {"S": assetid},
            "SK": {"S": assetid}
        },
        "UpdateExpression": "SET #f4dc0 = :f4dc0",
        "ExpressionAttributeNames": {"#f4dc0":"Status"},
        "ExpressionAttributeValues": {":f4dc0": {"S":status}}  #Status - AVAILABLE, IN_USE or LOW_BATTERY
    }


def execute_update_item(dynamodb_client, input): #this takes the input item from update_end_of_trip_trip_item and passes it as an item for an update_item call to trips
    try:
        response = dynamodb_client.update_item(**input)
        print("Successfully updated item.")
        # Handle response
    except ClientError as error:
        handle_error(error)
        raise
    except BaseException as error:
        print("Unknown error while updating item: ")
        raise

def handle_error(error):  # This is all error-handling built by NoSQL workshop standard code
    error_code = error.response['Error']['Code']
    error_message = error.response['Error']['Message']

    error_help_string = ERROR_HELP_STRINGS[error_code]

    print('[{error_code}] {help_string}. Error message: {error_message}'
          .format(error_code=error_code,
                  help_string=error_help_string,
                  error_message=error_message))

def lambda_handler(event, context):

    #low-level Client - must create a dynamodb client to talk to the tables
    dynamodb_client = create_dynamodb_client()


    vars = json.loads(event['body'])  # going to dump the body of either the POST or PUT into "vars" for parsing.  This function assumes whoever called the API knows or is generating the values
    assetid = vars['AssetID'] # Get the value of the assetID (the ENTIRE PK value, includingn ASSET# must be sent i.e. ASSET#S77818)
    tripsk = vars['TripSK'] # Get the trip SK (the ENTIRE SK value, including TRIP# must be sent i.e. TRIP#2023-08-29T16:02:46.228046#TCnr2)
    userid = vars['UserID'] # Get the UserID (the ENTIRE UserID value, including USER# must be sent, i.e. USER#1046)
    # The above three items should be part of any POST or PUT.  The rest depend on whether we're creating a new trip (POST) or updating an old one (PUT/else)
    if (event['httpMethod'] == 'POST'):
        tripstart = vars['StartTime']
        tripid = vars['TripID']
        create_trip_item_input = create_new_trip_item(userid, tripsk, tripid, tripstart)
        execute_create_trip_item(dynamodb_client, create_trip_item_input)
        update_bike_item_input = update_bike_item(assetid,"IN_USE")
        response = { 'status': 201, 'message': 'Trip created'}
    else:
        stoptime = vars['StopTime']
        money = vars['Fare']
        distance = vars['Miles']
        update_trip_item_input = update_end_of_trip_trip_item(userid,tripsk,stoptime,distance,money)
        execute_update_item(dynamodb_client, update_trip_item_input)
        update_bike_item_input = update_bike_item(assetid,"AVAILABLE")
        response = { 'status': 200, 'message': 'Trip updated'}

    # Now update the bike status with the value of the update_bike_item_input having been set above whether it was a POST or a PUT
    execute_update_item(dynamodb_client, update_bike_item_input)
    return {
        'statusCode': response['status'],
        'body': json.dumps({ "message": response['message']}),
        'headers': {"access-control-allow-origin" : "*" }
    }

