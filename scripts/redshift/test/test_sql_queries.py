import pytest
import psycopg2
import time
import boto3
import ujson as json

from botocore.exceptions import ClientError

DATABASE_NAME="corpus"

def get_secret():
    secret_name = 'redshift!redshift-cluster-1-awsuser'
    region_name = 'ap-northeast-2'
    
    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )

    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
    except ClientError as e:
        # For a list of exceptions thrown, see
        # https://docs.aws.amazon.com/secretsmanager/latest/apireference/API_GetSecretValue.html
        raise e

    secret = get_secret_value_response['SecretString']
    return secret

@pytest.fixture(scope="module")
def secret():
    return json.loads(get_secret())

@pytest.fixture(scope="module")
def s3():
    return boto3.client('s3')

@pytest.fixture(scope="module")
def db_connection(secret):
    conn = psycopg2.connect(
        dbname=DATABASE_NAME,
        user=secret['username'],
        password=secret['password'],
        port="5439",
        host='redshift-cluster-1.chzlcffktkrj.ap-northeast-2.redshift.amazonaws.com',
    )
    yield conn
    conn.close()

@pytest.fixture(scope="module")    
def query1_result(db_connection):
    query='''
    SELECT
        A.group_id,
        A.corpus_id AS "en_id", 
        B.corpus_id AS "zh_id", 
        (SELECT text FROM corpus WHERE id = A.corpus_ID) AS "en",
        (SELECT text FROM corpus WHERE id = B.corpus_ID) AS "zh",
        (SELECT title FROM projects 
        WHERE id=(
            SELECT project_id 
            FROM project_corpus_map 
            WHERE corpus_id = A.corpus_id 
            ORDER BY project_id DESC LIMIT 1)
        ) AS "en_delivery", -- 프로젝트 명
        (SELECT title FROM projects 
        WHERE id=(
            SELECT project_id 
            FROM project_corpus_map 
            WHERE corpus_id = A.corpus_id 
            ORDER BY project_id DESC LIMIT 1)
        ) AS "zh_delivery", -- 프로젝트 명
        
        COALESCE((
            SELECT name FROM tags 
            WHERE id=(
                SELECT tag_id 
                FROM group_tags_map 
                WHERE group_id=A.group_id AND priority = 1
            )), '') AS tag_1, -- #1 태그명
        COALESCE((
            SELECT score FROM group_tags_map
            WHERE group_id = A.group_id AND priority = 1
        ), 0) AS score_1, -- #1 태그명(점수)
        
        COALESCE((
            SELECT name FROM tags 
            WHERE id=(
                SELECT tag_id 
                FROM group_tags_map 
                WHERE group_id=A.group_id AND priority = 2
            )), '') AS tag_2, -- #2 태그명
        COALESCE((
            SELECT score FROM group_tags_map
            WHERE group_id = A.group_id AND priority = 2
        ), 0) AS score_2, -- #2 태그명(점수)
        
        COALESCE((
            SELECT name FROM tags 
            WHERE id=(
                SELECT tag_id 
                FROM group_tags_map 
                WHERE group_id=A.group_id AND priority = 3
            )), '') AS tag_3, -- #3 태그명
        COALESCE((
            SELECT score FROM group_tags_map
            WHERE group_id = A.group_id AND priority = 3
        ), 0) AS score_3 -- #3 태그명(점수)

    FROM 
        parallel_corpus A, parallel_corpus B

    WHERE 
        A.lang_id = 17 AND
        B.lang_id = 11 AND
        A.group_id = B.group_id

    ORDER BY
        A.group_id DESC
        
    '''
    with db_connection.cursor() as cursor:
        cursor.execute(query)
        return cursor.fetchall()
    
@pytest.fixture(scope="module")
def query2_result(db_connection):
    query='''
    SELECT
        A.group_id,
        A.corpus_id AS en_id,
        B.corpus_id AS zh_id,
        corpusA.text AS en,
        corpusB.text AS zh,
        project.title AS en_delivery,
        project.title AS zh_delivery,
        COALESCE(tag1.name, '') AS tag_1,
        COALESCE(gtm1.score, 0) AS score_1,
        COALESCE(tag2.name, '') AS tag_2,
        COALESCE(gtm2.score, 0) AS score_2,
        COALESCE(tag3.name, '') AS tag_3,
        COALESCE(gtm3.score, 0) AS score_3
    FROM 
        parallel_corpus A
        JOIN parallel_corpus B ON A.group_id = B.group_id AND A.lang_id = 17 AND B.lang_id = 11
        LEFT JOIN corpus corpusA ON A.corpus_id = corpusA.id
        LEFT JOIN corpus corpusB ON B.corpus_id = corpusB.id
        LEFT JOIN (
            SELECT corpus_id, MAX(project_id) AS max_project_id
            FROM project_corpus_map
            GROUP BY corpus_id
        ) pcm ON A.corpus_id = pcm.corpus_id
        LEFT JOIN projects project ON pcm.max_project_id = project.id
        LEFT JOIN group_tags_map gtm1 ON A.group_id = gtm1.group_id AND gtm1.priority = 1
        LEFT JOIN tags tag1 ON gtm1.tag_id = tag1.id
        LEFT JOIN group_tags_map gtm2 ON A.group_id = gtm2.group_id AND gtm2.priority = 2
        LEFT JOIN tags tag2 ON gtm2.tag_id = tag2.id
        LEFT JOIN group_tags_map gtm3 ON A.group_id = gtm3.group_id AND gtm3.priority = 3
        LEFT JOIN tags tag3 ON gtm3.tag_id = tag3.id
    
    '''
    start = time.time()
    with db_connection.cursor() as cursor:
        cursor.execute(query)
        end = time.time()
        print(end - start)
        return cursor.fetchall()

@pytest.mark.skip()    
def test_compare_two_queries(query1_result, query2_result):
    assert set(query1_result) == set(query2_result), "Query results should contain the same elements, regardless of order."

def test_query2_result(query2_result):
    print(query2_result)