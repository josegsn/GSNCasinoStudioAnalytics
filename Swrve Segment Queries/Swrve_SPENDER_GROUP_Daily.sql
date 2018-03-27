WITH new_spenders AS (
SELECT
	DISTINCT 
	a.user_id
FROM gsnmobile.events_dau a
LEFT JOIN gsnmobile.events_payments b
	ON a.user_id = b.user_id
	AND b.event_day <= date('{{ ds }}')
	AND b.user_id <> 'NULL'
	AND b.app = 'GSN Casino'
WHERE a.app = 'GSN Casino'
	AND a.event_day BETWEEN date('{{ ds }}') - 90 AND date('{{ ds }}')  
	AND a.user_id <> 'NULL'
GROUP BY 1
HAVING MIN(b.event_day) = date('{{ ds }}') )



SELECT
	swrve_user_id,
	'spender' AS SPENDER_GROUP

FROM new_spenders a
JOIN gsnmobile.swrve_casino_mapping y
	ON a.user_id = y.mesmoid
	