import boto3
import time
import json
import datetime

def create_log_stream(client, log_group_name, log_stream_name):
    print(f'log group {log_group_name} log stream {log_stream_name}')
    try:
        client.create_log_group(logGroupName=log_group_name)
        print(f'Log group {log_group_name} created successfully.')
    except client.exceptions.ResourceAlreadyExistsException:
        print(f'Log group {log_group_name} already exists.')
    try:
        client.create_log_stream(logGroupName=log_group_name, logStreamName=log_stream_name)
        print(f'Log stream {log_stream_name} created successfully.')
    except client.exceptions.ResourceAlreadyExistsException:
        print(f'Log stream {log_stream_name} already exists.')

def put_log_event(client, log_group_name, log_stream_name, message):
    timestamp = int(time.time() * 1000)
    response = client.describe_log_streams(logGroupName=log_group_name, logStreamNamePrefix=log_stream_name)
    log_stream = response['logStreams'][0]
    
    sequence_token = log_stream.get('uploadSequenceToken')

    log_event = {
        'logGroupName': log_group_name,
        'logStreamName': log_stream_name,
        'logEvents': [
            {
                'timestamp': timestamp,
                'message': message
            }
        ],
    }

    if sequence_token:
        log_event['sequenceToken'] = sequence_token

    response = client.put_log_events(**log_event)
    print(f'Log event sent successfully: {response}')

# Create log group and log stream if they don't exist
# # create_log_group(client, log_group_name)
# create_log_stream(client, log_group_name, log_stream_name)

# # # Sample record in comma-delimited format
# record = "field1,field2,field3,value1,value2,value3"
# put_log_event(client, log_group_name, log_stream_name, record)
