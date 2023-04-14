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
  if not os.path.isfile(fileName):
    objectName = os.path.basename(fileName)
    s3_client = boto3.client('s3')
    with open(fileName, 'wb') as fileSave:
      s3_client.download_fileobj(bucket, objectName, fileSave)
    # response = s3_client.get_object(Bucket=bucket, Key=objectName)
    # status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")
    # if status == 200:
    #     logging.info(f"Successful S3 get_object response. Status - {status}")
    #     response.put(Body=open(fileName, 'rb'))
    #     # return pd.read_csv(response.get("Body"))
    # else:
    #     logging.error(f"Unsuccessful S3 get_object response. Status - {status}")
    #     os.exit(0)
  return pd.read_csv(fileName).astype('float32')

# uploadFile('/csvFiles/tradeData-test.csv', 'crypto-bot-bucket')