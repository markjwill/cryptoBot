import boto3
import json

# Configure AWS credentials and region
aws_region = 'us-west-2'  # Change to your region
log_group_name = 'ML-Log-Group'
log_stream_name = '2024-05-30-06-09-stream'
output_file = 'log_messages.txt'

# Initialize Boto3 client for CloudWatch Logs
client = boto3.client('logs', region_name=aws_region)

def get_log_events(client, log_group_name, log_stream_name):
    log_events = []
    next_token = None

    while True:
        if next_token:
            response = client.get_log_events(
                logGroupName=log_group_name,
                logStreamName=log_stream_name,
                nextToken=next_token,
                limit=10000
            )
        else:
            response = client.get_log_events(
                logGroupName=log_group_name,
                logStreamName=log_stream_name,
                limit=10000
            )

        log_events.extend(response['events'])
        next_token = response.get('nextForwardToken')

        # Break if no new logs are available
        if not next_token or next_token == response['nextForwardToken']:
            break

    return log_events

def save_log_messages(log_events, output_file):
    with open(output_file, 'w') as f:
        for event in log_events:
            f.write(event['message'] + '\n')

# Retrieve log events
log_events = get_log_events(client, log_group_name, log_stream_name)

# Save log messages to file
save_log_messages(log_events, output_file)

print(f'Log messages have been saved to {output_file}')
