from __future__ import print_function
import boto3
import sys
import time
import threading
from multiprocessing import Queue
from botocore.exceptions import ClientError
import random
# from lab_config import boto_args
boto_args = {'service_name': 'dynamodb'}
dynamodb_client = boto3.client("dynamodb")

queue = Queue()

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

RETRYABLE_ERROR = ['InternalServerError','ProvisionedThroughputExceededException','ServiceUnavailable','RequestLimitExceeded','ThrottlingException']

def parallel_scan_update(tableName, totalsegments, threadsegment):
    dynamodb = boto3.resource(**boto_args)
    table = dynamodb.Table(tableName)

    totalbytessent = 0
    pageSize = 10000
    pje = 'PK,SK'
    
    items = []

    fe = "ServiceStatus = :s"
    eav = {":s": "OPEN"}

    response = table.scan(
        FilterExpression=fe,
        ExpressionAttributeValues=eav,
        Limit=pageSize,
        TotalSegments=totalsegments,
        Segment=threadsegment,
        ProjectionExpression=pje
        )

    # for i in response['Items']:
    items.extend(response['Items'])

    while 'LastEvaluatedKey' in response:
        response = table.scan(
            FilterExpression=fe,
            ExpressionAttributeValues=eav,
            Limit=pageSize,
            TotalSegments=totalsegments,
            Segment=threadsegment,
            ExclusiveStartKey=response['LastEvaluatedKey'],
            ProjectionExpression=pje)
        # for i in response['Items']:
        items.extend(response['Items'])

    queue.put(items)
    
    print(f'Updating service records {threadsegment}')
    
    for item in items:
        '''
         generating update-item request dictionary
        '''
        upd_item_request = {
            "TableName": tableName,
            "Key": { 'PK': { 'S': item.get('PK') } , 'SK' : { 'S': item.get('SK') } },
            "UpdateExpression": "SET GSI1_PK = :r",
            # "UpdateExpression": "REMOVE GS11_PK",
            "ExpressionAttributeValues": {':r': { 'S': 'OPEN#'+(item.get('PK')).split('#')[1] }}
            }
         
        ''' update gsi1_pk '''
        update_gsipk(tableName,upd_item_request)
    

def update_gsipk(table,upd_item_request):
    '''
    Function - Run UpdateItem
    '''
    
    resp_code = 1
    circ_break = 0
    
    while resp_code != 200:
        circ_break += 1
        # print({ 'PK': { 'S': item.get('PK') } , 'SK' : { 'S': item.get('SK') } })
        try:
            response = dynamodb_client.update_item(**upd_item_request)
            resp_code = 200
        except ClientError as error:
            retryable = handle_error(error)
            time.sleep((0.05*circ_break)+random.random())
            if circ_break > 10 or (not retryable):
               print('Hitting circuit breeker.')
               raise Exception('Exhausted retries or Non-Retryable error occured.')
               
                
def handle_error(error):
    error_code = error.response['Error']['Code']
    error_message = error.response['Error']['Message']

    error_help_string = ERROR_HELP_STRINGS[error_code]

    print('[{error_code}] {help_string}. Error message: {error_message}'
          .format(error_code=error_code,
                  help_string=error_help_string,
                  error_message=error_message))
    
    if error_code in RETRYABLE_ERROR:
        return True
    
    return False

if __name__ == "__main__":
    args = sys.argv[1:]
    tablename = args[0]
    total_segments = int(args[1])

    print(f'GSI Overloading in progress for {tablename}')

    begin_time = time.time()

    # BUGFIX https://github.com/boto/boto3/issues/1592
    boto3.resource(**boto_args)
    # 
    thread_list = []
    for i in range(total_segments):
        thread = threading.Thread(target=parallel_scan_update, args=(tablename, total_segments, i))
        thread.start()
        thread_list.append(thread)
        time.sleep(.1)

    for thread in thread_list:
        thread.join()
        
    totalbytessent = 0
    for i in range(total_segments):
        totalbytessent = totalbytessent + len(queue.get())
        
    print('GSI Overloading - Successful!!')