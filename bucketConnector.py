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
    logging.info(f'Downloading {fileName} from s3')
    objectName = os.path.basename(fileName)
    s3_client = boto3.client('s3')
    with open(fileName, 'wb') as fileSave:
      s3_client.download_fileobj(bucket, objectName, fileSave)

  logging.info(f'{fileName} is saved locally')
  return pd.read_csv(fileName)
