aws redshift-data execute-statement \
    --cluster-identifier redshift-cluster-1 \
    --database corpus \
    --db-user awsuser \
    --region ap-northeast-2 \
    --sql "CREATE TABLE voice_corpus ( \
            id integer \
           )"
#  AS \
#         SELECT * FROM s3://flitto/voice_corpus.csv \
#         IAM_ROLE 'arn:aws:iam::946551077663:role/service-role/AmazonRedshift-CommandsAccessRole-20231219T175533' \
#         FORMAT CSV \
#         IGNOREHEADER 1;"

# aws redshift-data describe-statement --id $statement_id