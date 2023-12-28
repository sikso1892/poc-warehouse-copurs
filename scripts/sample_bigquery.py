import os
import time

from glob import glob
from google.cloud import bigquery
from google.oauth2 import service_account

PATH_CREDENIAL_FILE = "/workspaces/poc-iac-bigquery-corpus/config/poc-bigquery-408603-c15bdbb69a0e.json"
PATH_SRC_DIR = "/workspaces/poc-iac-bigquery-corpus/data"

if __name__ == "__main__":    
    # 사용 예시
    credentials = service_account.Credentials.from_service_account_file(PATH_CREDENIAL_FILE)
    client = bigquery.Client(credentials=credentials, project=credentials.project_id, location="asia-northeast3")
    dataset = "poc_dataset_001"
    query = f"""
    SELECT
        COALESCE(en.group_id, ja.group_id, ko.group_id, zh.group_id) AS group_id,
        en.en AS en,
        ja.ja AS ja,
        ko.ko AS ko,
        zh.zh AS zh
    FROM
        `poc-bigquery-408603.poc_dataset_001.poc_corpus_en` en
    FULL OUTER JOIN
        `poc-bigquery-408603.poc_dataset_001.poc_corpus_ja` ja ON en.group_id = ja.group_id
    FULL OUTER JOIN 
        `poc-bigquery-408603.poc_dataset_001.poc_corpus_ko` ko ON COALESCE(en.group_id, ja.group_id) = ko.group_id
    FULL OUTER JOIN
        `poc-bigquery-408603.poc_dataset_001.poc_corpus_zh` zh ON COALESCE(en.group_id, ja.group_id, ko.group_id) = zh.group_id
    ORDER BY
        group_id
    """
    st = time.time()
    query_job = client.query(query)
    results = query_job.result()
    et = time.time()
    ext_time = et - st  
    print(ext_time)
