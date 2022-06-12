
from typing import List

import os
import boto3
from botocore.exceptions import ClientError


def aws_upload(files: List[str],
  aws_access_key_id: str,
  aws_secret_access_key: str,
  region_name: str,
  bucket: str):
    """
    Uploads a list of files to S3.
    """
    client = boto3.client(
      's3',
      region_name=region_name,
      aws_access_key_id=aws_access_key_id,
      aws_secret_access_key=aws_secret_access_key
    )

    response = {}

    _files = [str(x) for x in files]

    for file in _files:
      try:
        client.upload_file(file, bucket, os.path.basename(file).lower())
        response[file] = True
      except ClientError as e:
        print(e)
        response[file] = False

    return response
