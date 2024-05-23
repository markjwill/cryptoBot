import boto3
import time
import json
import datetime
import configparser
import os

config = configparser.ConfigParser()


os.environ['AWS_ACCESS_KEY_ID'] = config.get('default', 'aws_access_key_id')
os.environ['AWS_SECRET_ACCESS_KEY'] = config.get('default', 'aws_secret_access_key')

class cloudLogger:
    def __init__(self, aws_region, log_group_name):
        self.client = boto3.client('logs', region_name=aws_region)
        self.log_group_name = log_group_name
        self.log_stream_name = f'{datetime.datetime.now().strftime("%Y-%m-%d-%H-%M")}-stream'
        self.create_log_stream()

    def create_log_stream(self):
        print(f'Creating log group {self.log_group_name} and log stream {self.log_stream_name}')
        try:
            self.client.create_log_group(logGroupName=self.log_group_name)
            print(f'Log group {self.log_group_name} created successfully.')
        except self.client.exceptions.ResourceAlreadyExistsException:
            print(f'Log group {self.log_group_name} already exists.')
        
        try:
            self.client.create_log_stream(logGroupName=self.log_group_name, logStreamName=self.log_stream_name)
            print(f'Log stream {self.log_stream_name} created successfully.')
        except self.client.exceptions.ResourceAlreadyExistsException:
            print(f'Log stream {self.log_stream_name} already exists.')

    def log(self, message):
        timestamp = int(time.time() * 1000)
        response = self.client.describe_log_streams(logGroupName=self.log_group_name, logStreamNamePrefix=self.log_stream_name)
        log_stream = response['logStreams'][0]
        
        sequence_token = log_stream.get('uploadSequenceToken')

        log_event = {
            'logGroupName': self.log_group_name,
            'logStreamName': self.log_stream_name,
            'logEvents': [
                {
                    'timestamp': timestamp,
                    'message': message
                }
            ],
        }

        if sequence_token:
            log_event['sequenceToken'] = sequence_token

        response = self.client.put_log_events(**log_event)
        print(f'Log event sent successfully: {response}')

# cloudLogger = cloudLogger('us-west-2', 'ML-Log-Group')
# cloudLogger.log("test,log,row")
