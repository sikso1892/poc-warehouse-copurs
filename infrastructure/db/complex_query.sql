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
	