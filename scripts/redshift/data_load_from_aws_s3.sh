#!/bin/bash

# AWS CLI를 통해 S3 파일 리스트 조회
AWS_REGION="ap-northeast-2"
S3_BUCKET="flitto"

REDSHIFT_CLUSTER="redshift-cluster-1"
IAM_ROLE_ARN="arn:aws:iam::946551077663:role/service-role/AmazonRedshift-CommandsAccessRole-20231219T175533"

DATABASE_NAME="corpus"
DATABASE_USER="awsuser"

file_list=$(aws s3 ls s3://$S3_BUCKET/ --region $AWS_REGION | grep ".csv")

# 파일 리스트 출력
echo "Files in $S3_BUCKET/$S3_PREFIX:"
echo "$file_list"

# 파일 리스트에서 파일 이름 추출
file_names=$(echo "$file_list" | awk '{print $4}')

# 파일 이름을 이용하여 S3 URL 생성
for file_name in $file_names; do
    s3_url="s3://$S3_BUCKET/$S3_PREFIX/$file_name"
    table_name=${file_name%.*}

    echo "S3 URL for $file_name: $s3_url"
    echo "Table Name: $table_name"

    statement_id=$(aws redshift-data execute-statement \
        --cluster-identifier $REDSHIFT_CLUSTER \
        --database $DATABASE_NAME \
        --db-user $DATABASE_USER \
        --region $AWS_REGION \
        --sql "CREATE TABLE $table_name AS SELECT * FROM $s3_url IAM_ROLE '$IAM_ROLE_ARN' FORMAT CSV IGNOREHEADER 1;" \
        | jq -r '.Id')

    aws redshift-data describe-statement --id $statement_id

done
