#!/bin/bash

BUCKET_NAME="poc-copurs-bucket"
DATASET_NAME="poc_dataset_002"

# GCS 버킷에서 CSV 파일 목록을 가져옵니다.
FILES=$(gsutil ls gs://${BUCKET_NAME}/*.csv)

# 각 CSV 파일에 대해 반복하여 BigQuery 테이블을 생성합니다.
for FILE in $FILES; do
  # 파일 이름에서 경로를 제외하고 확장자를 제거하여 테이블 이름을 생성합니다.
  TABLE_NAME=$(basename $FILE .csv)
  
  # BigQuery로 데이터를 로드합니다.
  bq load --autodetect --source_format=CSV ${DATASET_NAME}.${TABLE_NAME} $FILE
done