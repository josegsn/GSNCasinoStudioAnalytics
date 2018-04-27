WITH segments AS (
SELECT 
	DISTINCT
	attr3 AS mesmo_id,
	CASE
		WHEN REGEXP_SUBSTR(attr3, '\d\d$') BETWEEN 00 AND 19 THEN 'A'
		WHEN REGEXP_SUBSTR(attr3, '\d\d$') BETWEEN 20 AND 39 THEN 'B'
		WHEN REGEXP_SUBSTR(attr3, '\d\d$') BETWEEN 40 AND 59 THEN 'C'
		WHEN REGEXP_SUBSTR(attr3, '\d\d$') BETWEEN 60 AND 79 THEN 'D'
		WHEN REGEXP_SUBSTR(attr3, '\d\d$') BETWEEN 80 AND 99 THEN 'E'
		END AS segment


FROM gsnmobile.events
WHERE app_name = 'GSN Casino'
	AND event_name = 'checkpoint'	
	AND event_day BETWEEN date('{{ ds }}') - 30 AND date('{{ ds }}')
	AND attr3 IS NOT NULL
)


SELECT
	DISTINCT
	swrve_user_id as "user",
	segment as TESTING_SEGMENT
FROM segments a

JOIN gsnmobile.swrve_casino_mapping y
	ON a.mesmo_id = y.mesmoid
LIMIT 10;