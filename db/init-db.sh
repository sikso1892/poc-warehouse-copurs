#!/bin/bash

# 데이터베이스 생성
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    CREATE DATABASE $POSTGRES_DB;
EOSQL

# 생성된 데이터베이스에 접속
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    -- 여기에 테이블 생성 쿼리를 추가합니다.
    -- 예: CREATE TABLE my_table (id SERIAL PRIMARY KEY, name VARCHAR(100));
EOSQL
