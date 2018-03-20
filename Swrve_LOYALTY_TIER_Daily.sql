
WITH segments AS (
SELECT 
	synthetic_id,
	MAX(CASE 
		WHEN attr30 IS NULL THEN 1
		WHEN attr30 = 'BRONZE' THEN 1
		WHEN attr30 = 'SILVER' THEN 2
		WHEN attr30 = 'GOLD' THEN 3
		WHEN attr30 = 'DIAMOND' THEN 4
		WHEN attr30 = 'BLUEDIAMOND' THEN 5
		WHEN attr30 = 'PLATINUM' THEN 5
		END) AS tier_today,

	IFNULL(MAX(CASE 
			WHEN event_day = date('{{ ds }}') - 1 THEN 0
			WHEN attr30 IS NULL THEN 1
			WHEN attr30 = 'BRONZE' THEN 1
			WHEN attr30 = 'SILVER' THEN 2
			WHEN attr30 = 'GOLD' THEN 3
			WHEN attr30 = 'DIAMOND' THEN 4
			WHEN attr30 = 'BLUEDIAMOND' THEN 5
			WHEN attr30 = 'PLATINUM' THEN 5
			END),0) AS tier_last

FROM gsnmobile.events
WHERE app_name = 'GSN Casino'
	AND attr20 = 'Loyalty'	
	AND event_day BETWEEN date('{{ ds }}') - 120 AND date('{{ ds }}') - 1
GROUP BY 1)

SELECT
	DISTINCT swrve_user_id,
	CASE 
		WHEN tier_today = 1 THEN 'BRONZE'
		WHEN tier_today = 2 THEN 'SILVER'
		WHEN tier_today = 3 THEN 'GOLD'
		WHEN tier_today = 4 THEN 'DIAMOND'
		WHEN tier_today = 5 THEN 'BLUEDIAMOND'
		WHEN tier_today = 6 THEN 'PLATINUM'
	END as LOYALTY_TIER
FROM segments
JOIN gsnmobile.dim_device_mesmoids m 
	USING(synthetic_id)
JOIN gsnmobile.swrve_casino_mapping y
	ON m.mesmo_id = y.mesmoid
WHERE m.app_name = 'GSN Casino'
	AND tier_today > tier_last