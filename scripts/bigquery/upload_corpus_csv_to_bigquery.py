'''
csv 파일로 부터 
'''
from google.cloud import bigquery
from google.cloud.bigquery import LoadJobConfig, SourceFormat
from google.oauth2 import service_account

from glob import glob

import os

PATH_CREDENIAL_FILE = "config/poc-bigquery-408603-c15bdbb69a0e.json"

# BigQuery 클라이언트 설정
credentials = service_account.Credentials.from_service_account_file(PATH_CREDENIAL_FILE)
client = bigquery.Client(
    credentials=credentials, project=credentials.project_id, location="asia-northeast3"
)

# 대상 데이터셋과 테이블 지정
dataset_id = "poc_dataset_002"
table_id = "corpus"
table_ref = client.dataset(dataset_id).table(table_id)

# 로드할 CSV 파일들의 경로
# 분할된 파일 목록
file_paths = [
    file
    for file in sorted(
        glob(
            "/Users/pc-2312002/work/poc-warehouse-copurs/poc-on-prem-corpus/dump/2023-12-26/corpus/*.csv"
        )
    )
]

# 로드 작업 구성
job_config = LoadJobConfig()
job_config.source_format = SourceFormat.CSV
job_config.skip_leading_rows = 1  # 첫 번째 행이 헤더인 경우
job_config.write_disposition = "WRITE_APPEND"  # 기존 테이블에 데이터 추가

# 각 CSV 파일을 순차적으로 로드
for file_path in file_paths:
    with open(file_path, "rb") as source_file:
        job = client.load_table_from_file(source_file, table_ref, job_config=job_config)
        job.result()  # 작업 완료 대기

    print(f"{file_path} has been loaded to {dataset_id}.{table_id}")
