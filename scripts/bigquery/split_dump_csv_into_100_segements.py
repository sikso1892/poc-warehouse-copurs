#!/bin/env python
import polars as pl
import math
import os

# 저장 경로
PATH_TO_DIR = "/Users/pc-2312002/work/poc-warehouse-copurs/poc-on-prem-corpus/dump/2023-12-26/corpus"

# 원본 CSV 파일
input_file = "/Users/pc-2312002/work/poc-warehouse-copurs/poc-on-prem-corpus/dump/2023-12-26/corpus.csv"

# 데이터 프레임 로드
df = pl.read_csv(input_file)

# 전체 행 수 계산
total_rows = df.height

# 각 파일에 들어갈 행의 수
rows_per_file = math.ceil(total_rows / 100)

# 100등분하여 파일로 저장
for i in range(100):
    start_row = i * rows_per_file
    end_row = start_row + rows_per_file

    # 데이터 프레임 슬라이싱
    df_subset = df.slice(start_row, rows_per_file)

    # zerofill 파일명 생성 (예: corpus_part_001.csv)
    file_name = os.path.join(PATH_TO_DIR, f"corpus_part_{str(i+1).zfill(3)}.csv")

    # 분할된 데이터를 새로운 파일로 저장
    df_subset.write_csv(file_name)
