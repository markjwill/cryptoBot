import logging
import boto3
from botocore.exceptions import ClientError
import os
import pandas as pd


def uploadFile(fileName, bucket):
  objectName = os.path.basename(fileName)
  s3_client = boto3.client('s3')
  try:
    response = s3_client.upload_file(fileName, bucket, objectName)
  except ClientError as e:
    logging.error(e)
    return False
  return True


def downloadFile(fileName, bucket):
  objectName = os.path.basename(fileName)
  s3_client = boto3.client('s3')
  response = s3_client.get_object(Bucket=bucket, Key=objectName)
  status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")
  if status == 200:
      logging.info(f"Successful S3 get_object response. Status - {status}")
      return pd.read_csv(response.get("Body"))
  else:
      logging.error(f"Unsuccessful S3 get_object response. Status - {status}")
      os.exit(0)

# uploadFile('/csvFiles/tradeData-test.csv', 'crypto-bot-bucket')