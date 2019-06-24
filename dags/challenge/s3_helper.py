import logging

import boto3

import json

import os


def write_to_s3(message_value: str, s3_location: str, file_name: str):
    """
    Writes the specified data to s3
    Parameters
    --------
    message_value : Message/data that needs to be written to s3
    s3_location : Location of the file on s3 where the data needs to be saved
    file_name: The name of file that needs to be saved on s3

    Examples
    --------
    write_to_s3(file ,"mybucket/filename.json" )
    """
    ACCESS_KEY = os.environ['AWS_ACCESS_KEY']
    SECRET_KEY = os.environ['AWS_SECRET_KEY']

    # json_data = message_value.encode()
    s3 = boto3.resource('s3',
                        aws_access_key_id=ACCESS_KEY,
                        aws_secret_access_key=SECRET_KEY
                        )
    bucket = 'airflow-challenge-new-api'
    # bucket = config.bucket_name
    logging.info('writing' + file_name + 'to s3.. at location' + s3_location)
    data = s3_location + "/" + file_name
    s3object = s3.Object(bucket, data)
    s3object.put(
        Body=(message_value.encode('UTF-8'))
    )
