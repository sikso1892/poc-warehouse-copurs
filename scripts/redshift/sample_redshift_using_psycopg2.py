import psycopg2
# Use this code snippet in your app.
# If you need more information about configurations
# or implementing the sample code, visit the AWS docs:
# https://aws.amazon.com/developer/language/python/

import boto3
from botocore.exceptions import ClientError

import configparser
import json

PATH_CONFIG = "poc-iac-bigquery-corpus/config/poc-redshift.ini"

config = configparser.ConfigParser()
config.read(PATH_CONFIG)

def get_secret():

    secret_name = config['secret']['name']
    region_name = config['secret']['region']

    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )

    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
    except ClientError as e:
        # For a list of exceptions thrown, see
        # https://docs.aws.amazon.com/secretsmanager/latest/apireference/API_GetSecretValue.html
        raise e

    secret = get_secret_value_response['SecretString']
    return secret
    # Your code goes here.
    
secret = json.loads(get_secret())

conn = psycopg2.connect(
    dbname=config['redshift']['dbname'],
    user=secret['username'],
    password=secret['password'],
    host=config['redshift']['endpoint'],
    port=5439
)

cursor = conn.cursor()
query = """
SELECT
    COALESCE(en.group_id, ja.group_id, ko.group_id, zh.group_id) AS group_id,
    en.en AS en,
    ja.ja AS ja,
    ko.ko AS ko,
    zh.zh AS zh
FROM
     public.en en
FULL OUTER JOIN
	   public.ja ja ON en.group_id = ja.group_id
FULL OUTER JOIN 
     public.ko ko ON COALESCE(en.group_id, ja.group_id) = ko.group_id
FULL OUTER JOIN
     public.zh zh ON COALESCE(en.group_id, ja.group_id, ko.group_id) = zh.group_id
ORDER BY
    group_id
"""
cursor.execute(query)
result = cursor.fetchall()