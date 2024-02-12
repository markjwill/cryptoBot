import logging
import boto3
from botocore.exceptions import ClientError
import os
import pandas as pd
import progressbar
from botocore.exceptions import NoCredentialsError

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
  df = pd.read_csv(fileName)
  pd.set_option('display.max_rows', 20)
  logging.debug(df.dtypes)
  return df

def download_from_s3(bucket, s3_file, local_file):
    s3 = boto3.client('s3')
    response = s3.head_object(Bucket=bucket, Key=s3_file)
    size = response['ContentLength']
    downloadProgress = progressbar.progressbar.ProgressBar(maxval=size)
    downloadProgress.start()

    def download_progress(chunk):
        downloadProgress.update(downloadProgress.currval + chunk)

    try:
        s3.download_file(bucket, s3_file, local_file, Callback=download_progress)
        downloadProgress.finish()
        print("Download Successful")
        return True
    except FileNotFoundError:
        print("The file was not found")
        return False
    except NoCredentialsError:
        print("Credentials not available")
        return False
