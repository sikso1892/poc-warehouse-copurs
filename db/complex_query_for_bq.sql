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
    `poc_dataset_002.parallel_corpus` A
    JOIN `poc_dataset_002.parallel_corpus` B ON A.group_id = B.group_id AND A.lang_id = 17 AND B.lang_id = 11
    LEFT JOIN `poc_dataset_002.corpus` corpusA ON A.corpus_id = corpusA.id
    LEFT JOIN `poc_dataset_002.corpus` corpusB ON B.corpus_id = corpusB.id
    LEFT JOIN (
        SELECT corpus_id, MAX(project_id) AS max_project_id
        FROM `poc_dataset_002.project_corpus_map`
        GROUP BY corpus_id
    ) pcm ON A.corpus_id = pcm.corpus_id
    LEFT JOIN `poc_dataset_002.projects` project ON pcm.max_project_id = project.id
    LEFT JOIN `poc_dataset_002.group_tags_map` gtm1 ON A.group_id = gtm1.group_id AND gtm1.priority = 1
    LEFT JOIN `poc_dataset_002.tags` tag1 ON gtm1.tag_id = tag1.id
    LEFT JOIN `poc_dataset_002.group_tags_map` gtm2 ON A.group_id = gtm2.group_id AND gtm2.priority = 2
    LEFT JOIN `poc_dataset_002.tags` tag2 ON gtm2.tag_id = tag2.id
    LEFT JOIN `poc_dataset_002.group_tags_map` gtm3 ON A.group_id = gtm3.group_id AND gtm3.priority = 3
    LEFT JOIN `poc_dataset_002.tags` tag3 ON gtm3.tag_id = tag3.id
