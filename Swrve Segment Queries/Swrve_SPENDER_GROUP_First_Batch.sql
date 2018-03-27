SELECT
	DISTINCT 
	swrve_user_id,
	CASE WHEN b.user_id IS NOT NULL THEN 'spender' ELSE 'non_spender' END AS SPENDER_GROUP
FROM gsnmobile.events_dau a
JOIN gsnmobile.swrve_casino_mapping y
	ON a.user_id = y.mesmoid
LEFT JOIN gsnmobile.events_payments b
	ON a.user_id = b.user_id
	AND b.event_day < date('{{ ds }}')
	AND b.user_id <> 'NULL'
	AND b.app = 'GSN Casino'
WHERE a.app = 'GSN Casino'
	AND a.event_day BETWEEN date('{{ ds }}') - 90 AND date('{{ ds }}') 
	AND a.user_id <> 'NULL'
