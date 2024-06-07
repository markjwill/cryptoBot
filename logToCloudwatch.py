import boto3
import time
import json
import datetime
from threading import Thread

class CloudLogger:
    def __init__(self, aws_region, log_group_name):
        self.client = boto3.client(
            'logs', 
            region_name=aws_region,
            aws_access_key_id='Key',
            aws_secret_access_key='Secret'
        )
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
        Thread(target=self.threaded_log, args=(message, )).start()
        # print(f'Log event sent successfully: {response}')

    def threaded_log(self, message):
        attempts = 0
        logSuccess = False
        exceptionToken = False
        timestamp = int(time.time() * 1000)
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
        while not logSuccess:
            attempts += 1
            if attempts > 10:
                print("production run ending in shutdown")
                return False
                # os.system("shutdown now -h")
            try:
                response = self.client.describe_log_streams(logGroupName=self.log_group_name, logStreamNamePrefix=self.log_stream_name)
                print('got stream response')
                log_stream = response['logStreams'][0]
                print('got log stream')
                sequence_token = log_stream.get('uploadSequenceToken')
                print('got sequence token')
                exceptionToken = False

                if sequence_token:
                    log_event['sequenceToken'] = sequence_token
                    print('inserted sequence token')

                    response = self.client.put_log_events(**log_event)
                    logSuccess = True
                    print(f'Log event sent successfully: {response}')
                print(f'sequence token didn\'t')
            except Exception as e:
                print(f'Error logging event: {e}')



# cloudLogger = cloudLogger('us-west-2', 'ML-Log-Group')
# cloudLogger.log("test,log,row")
