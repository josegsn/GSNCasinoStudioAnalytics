
WITH segments AS (
SELECT 
	synthetic_id,
	MAX(attr3*1) AS mesmo_id,
	MAX(CASE 
		WHEN attr30 IS NULL THEN 1
		WHEN attr30 = 'BRONZE' THEN 1
		WHEN attr30 = 'SILVER' THEN 2
		WHEN attr30 = 'GOLD' THEN 3
		WHEN attr30 = 'DIAMOND' THEN 4
		WHEN attr30 = 'BLUEDIAMOND' THEN 5
		WHEN attr30 = 'PLATINUM' THEN 6
		END) AS tier_today,

	IFNULL(MAX(CASE 
			WHEN event_day = date('{{ ds }}')  THEN 0
			WHEN attr30 IS NULL THEN 1
			WHEN attr30 = 'BRONZE' THEN 1
			WHEN attr30 = 'SILVER' THEN 2
			WHEN attr30 = 'GOLD' THEN 3
			WHEN attr30 = 'DIAMOND' THEN 4
			WHEN attr30 = 'BLUEDIAMOND' THEN 5
			WHEN attr30 = 'PLATINUM' THEN 6
			END),0) AS tier_last

FROM gsnmobile.events
WHERE app_name = 'GSN Casino'
	AND attr20 = 'Loyalty'	
	AND event_day BETWEEN date('{{ ds }}') - 120 AND date('{{ ds }}') 
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
FROM segments a

JOIN gsnmobile.swrve_casino_mapping y
	ON a.mesmo_id = y.mesmoid
WHERE tier_today > tier_last
