import pytest
import psycopg2
import configparser
import logging
@pytest.fixture(scope="module")
def db_config():
    config = configparser.ConfigParser()
    config.read('config/poc-postgres.ini')
    return config['postgres.devel']

@pytest.fixture(scope="module")
def db_connection(db_config):
    conn = psycopg2.connect(
        host=db_config['host'],
        port=db_config['port'],
        database=db_config['database'],
        user=db_config['user'],
        password=db_config['password']
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
    with db_connection.cursor() as cursor:
        cursor.execute(query)
        return cursor.fetchall()
    
def test_compare_two_queries(query1_result, query2_result):
    assert set(query1_result) == set(query2_result), "Query results should contain the same elements, regardless of order."
