#!/bin/bash

# AWS CLI를 통해 S3 파일 리스트 조회
AWS_REGION="ap-northeast-2"
S3_BUCKET="flitto"

REDSHIFT_CLUSTER="redshift-cluster-1"
IAM_ROLE_ARN="arn:aws:iam::946551077663:role/service-role/AmazonRedshift-CommandsAccessRole-20231219T175533"


# 파일 이름을 이용하여 S3 URL 생성
aws redshift-data list-statements
