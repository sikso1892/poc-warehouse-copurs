from google.cloud import bigquery
from google.cloud.bigquery import LoadJobConfig, SourceFormat
from google.oauth2 import service_account

from glob import glob

import os
import csv
from tqdm import tqdm
from itertools import islice
from pprint import pprint as print

from google.cloud import bigquery

PATH_CREDENIAL_FILE = "/Users/pc-2312002/work/poc-warehouse-copurs/config/poc-bigquery-408603-c15bdbb69a0e.json"
PATH_FILE = '/Users/pc-2312002/work/poc-warehouse-copurs/poc-on-prem-corpus/dump/2023-12-26/corpus.csv'

BATCH_SIZE = 10000
# BigQuery 클라이언트 설정
credentials = service_account.Credentials.from_service_account_file(PATH_CREDENIAL_FILE)
client = bigquery.Client(credentials=credentials, project=credentials.project_id, location="asia-northeast3")

# 대상 데이터셋과 테이블 지정
dataset_id = 'poc_dataset_002'
table_id = 'corpus'
table_ref = client.dataset(dataset_id).table(table_id)

table = client.get_table(                  )  # API 요청

# 테이블 스키마 출력
print("Table schema: {}".format(table.schema))

with open(PATH_FILE, "r") as csv_file:
    csv_reader = csv.reader(csv_file)
    
    rows_to_insert = []
    
    for idx, row in tqdm(enumerate(islice(csv_reader, 1, None))):
        
        if len(row) != 3:
            print(row)
            break
        
        rows_to_insert.append(
            {
                "id": int(row[0]),
                "lang_id": int(row[1]),
                "text": row[2]
            }
        )
        
        if len(rows_to_insert) == BATCH_SIZE:
            errors = client.insert_rows(table_ref, rows_to_insert, selected_fields=table.schema)

            if errors:
                print("Errors occurred while inserting rows: {}".format(errors))
                
            rows_to_insert = []

    if rows_to_insert:
        errors = client.insert_rows(table_ref, rows_to_insert, selected_fields=table.schema)

        if errors:
            print("Errors occurred while inserting rows: {}".format(errors))
        