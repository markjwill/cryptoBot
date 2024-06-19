import boto3
import time
import json
import datetime
from threading import Thread



class CloudLogger:
    def __init__(self, aws_region, log_group_name, columns):
        self.client = boto3.client(
            'logs', 
            region_name=aws_region
        )
        self.log_group_name = log_group_name
        self.log_stream_name = f'{datetime.datetime.now().strftime("%Y-%m-%d-%H-%M")}-stream'
        self.sequence_token = None
        self.batch_size = 200
        self.log_events = []
        self.create_log_stream()
        self.log(columns)
        self.flush()

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
        log_event = {
            'timestamp': timestamp,
            'message': message
        }
        self.log_events.append(log_event)

        if len(self.log_events) >= self.batch_size:
            self.put_log_events()

    def put_log_events(self):
        if not self.log_events:
            return

        kwargs = {
            'logGroupName': self.log_group_name,
            'logStreamName': self.log_stream_name,
            'logEvents': self.log_events,
        }
        if self.sequence_token:
            kwargs['sequenceToken'] = self.sequence_token

        response = self.client.put_log_events(**kwargs)
        self.sequence_token = response['nextSequenceToken']
        self.log_events = []

    def flush(self):
        if self.log_events:
            self.put_log_events()


# cloudLogger = cloudLogger('us-west-2', 'ML-Log-Group')
# cloudLogger.log("test,log,row")
