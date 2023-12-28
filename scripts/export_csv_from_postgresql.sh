#!/bin/bash

TODAY=$(date +"%Y-%m-%d")
PATH_DUMP_DIR=/var/lib/postgresql/dump/$TODAY
mkdir -p $PATH_DUMP_DIR

# 테이블 목록을 가져옵니다.
TABLES=$(psql -d $POSTGRES_DB -U $POSTGRES_USER -t -c "SELECT tablename FROM pg_catalog.pg_tables WHERE schemaname = 'public'")

# 각 테이블을 CSV로 덤프합니다.
for TABLE in $TABLES; do
    psql -d $POSTGRES_DB -U $POSTGRES_USER -c "COPY public.$TABLE TO '$PATH_DUMP_DIR/$TABLE.csv' DELIMITER ',' CSV HEADER"
done
