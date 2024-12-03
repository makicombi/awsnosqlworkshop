
# Load the AWS SDK for Python
import json
import boto3
from boto3.dynamodb.conditions import Key, Attr
import os
from botocore.exceptions import ClientError

ERROR_HELP_STRINGS = {
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


def create_dynamodb_client(): # Instigate a dynamodb client for talking to tables. 
    region=boto3.session.Session().region_name
    return boto3.client("dynamodb", region_name=region)


def create_query_input(userid):  #this creates the PK for the query by adding to the userId sent in from the GET, then creates a dict that matches a format that boto3 likes for querying tables
    userstring='USER#'+userid
    return {
        "TableName": "trips",
        "KeyConditionExpression": "#cd420 = :cd420 And begins_with(#cd421, :cd421)",
        "ScanIndexForward": False,
        "Limit": 10,
        "ExpressionAttributeNames": {"#cd420":"PK","#cd421":"SK"},
        "ExpressionAttributeValues": {":cd420": {"S":userstring},":cd421": {"S":"TRIP#"}}
    }


def execute_query(dynamodb_client, input):  #this simply takes the query input dict built by create_query_input and performs the actual query using the client
    try:
        response = dynamodb_client.query(**input)
        print("Query successful.")
        return response # Handle response
    except ClientError as error:
        handle_error(error)
    except BaseException as error:
        print("Unknown error while querying: ")
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

    # Create the dictionary containing arguments for query call using GET input. Note the GET query string must match UserId=VALUE format. i.e. UserId=1046
    userID = (event['queryStringParameters']['UserId'])
    query_input = create_query_input(userID)

    # Call DynamoDB's query API and return the items from the response back to the API
    returnval = execute_query(dynamodb_client, query_input)
    return {
        'statusCode': 200,
        'body': json.dumps(returnval['Items'])
    }
