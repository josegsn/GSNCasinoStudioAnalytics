WITH values_user AS(
SELECT
	synthetic_id,
	
	CASE 
		WHEN IFNULL(COUNT(DISTINCT CASE 
									WHEN event_day BETWEEN date('{{ ds }}') - 7  AND date('{{ ds }}') - 1  
									THEN synthetic_id 
									ELSE NULL END),0) = 0 
	    THEN 0 
	    ELSE 
			SUM(CASE 
					WHEN event_day BETWEEN date('{{ ds }}') - 7  AND date('{{ ds }}') - 1 
			    				AND event_name = 'adjustTokenBalance' 
			    				AND attr17*1 < 0 
					THEN 1 
					ELSE 0 END)/
			COUNT(DISTINCT CASE 
				WHEN event_day BETWEEN date('{{ ds }}') - 7  AND date('{{ ds }}') - 1  
				THEN synthetic_id 
				ELSE NULL END)  END AS spins,
						
	CASE 
		WHEN IFNULL(COUNT(DISTINCT CASE 
									WHEN event_day BETWEEN date('{{ ds }}') - 8  AND date('{{ ds }}') - 2  
									THEN synthetic_id 
									ELSE NULL END),0) = 0 
	    THEN 0 
	    ELSE 
			SUM(CASE 
					WHEN event_day BETWEEN date('{{ ds }}') - 8  AND date('{{ ds }}') - 2 
			    				AND event_name = 'adjustTokenBalance' 
			    				AND attr17*1 < 0 
					THEN 1 
					ELSE 0 END)/
			COUNT(DISTINCT CASE 
				WHEN event_day BETWEEN date('{{ ds }}') - 8  AND date('{{ ds }}') - 2  
				THEN synthetic_id 
				ELSE NULL END)  END AS spins_pre,			

	
	CASE 
		WHEN IFNULL(COUNT(DISTINCT CASE 
									WHEN event_day BETWEEN date('{{ ds }}') - 7  AND date('{{ ds }}') - 1  
									THEN synthetic_id 
									ELSE NULL END),0) = 0 
	    THEN 0 
	    ELSE 
			SUM(CASE 
					WHEN event_day BETWEEN date('{{ ds }}') - 7  AND date('{{ ds }}') - 1 
			    				AND event_name = 'adjustTokenBalance' 
			    				AND attr17*1 < 0 
					THEN attr17*-1 
					ELSE 0 END)/
			COUNT(DISTINCT CASE 
				WHEN event_day BETWEEN date('{{ ds }}') - 7  AND date('{{ ds }}') - 1  
				THEN synthetic_id 
				ELSE NULL END)  END AS tokens,
						
	CASE 
		WHEN IFNULL(COUNT(DISTINCT CASE 
									WHEN event_day BETWEEN date('{{ ds }}') - 8  AND date('{{ ds }}') - 2  
									THEN synthetic_id 
									ELSE NULL END),0) = 0 
	    THEN 0 
	    ELSE 
			SUM(CASE 
					WHEN event_day BETWEEN date('{{ ds }}') - 8  AND date('{{ ds }}') - 2 
			    				AND event_name = 'adjustTokenBalance' 
			    				AND attr17*1 < 0 
					THEN attr17*-1 
					ELSE 0 END)/
			COUNT(DISTINCT CASE 
				WHEN event_day BETWEEN date('{{ ds }}') - 8  AND date('{{ ds }}') - 2  
				THEN synthetic_id 
				ELSE NULL END)  END AS tokens_pre
FROM gsnmobile.events
WHERE event_name IN ('adjustTokenBalance', 'getConfiguration')
	AND event_day BETWEEN date('{{ ds }}') - 8 AND date('{{ ds }}') - 1
GROUP BY 1)


,distributions AS (
SELECT
	swrve_user_id,
	NTILE(100) OVER (ORDER BY spins*1 ASC) AS spins,
	NTILE(100) OVER (ORDER BY tokens*1 ASC) AS tokens,
	NTILE(100) OVER (ORDER BY spins_pre*1 ASC) AS spins_pre,
	NTILE(100) OVER (ORDER BY tokens_pre*1 ASC) AS tokens_pre

FROM values_user a
JOIN gsnmobile.dim_device_mesmoids m 
	USING(synthetic_id)
JOIN gsnmobile.swrve_casino_mapping y
	ON m.mesmo_id = y.mesmoid)

, final AS (	
SELECT
	swrve_user_id,
	
	CASE 
		WHEN MAX(spins) BETWEEN 0 AND 64 THEN 'LOW'
		WHEN MAX(spins) BETWEEN 65 AND 94 THEN 'MID'
		WHEN MAX(spins) BETWEEN 95 AND 100 THEN 'HIGH'
	END AS SPINS_SEGMENT,

	CASE 
		WHEN MAX(tokens) BETWEEN 0 AND 64 THEN 'LOW'
		WHEN MAX(tokens) BETWEEN 65 AND 94 THEN 'MID'
		WHEN MAX(tokens) BETWEEN 95 AND 100 THEN 'HIGH'
	END AS TOKENS_SEGMENT,
	
	CASE 
		WHEN MAX(spins_pre) BETWEEN 0 AND 64 THEN 'LOW'
		WHEN MAX(spins_pre) BETWEEN 65 AND 94 THEN 'MID'
		WHEN MAX(spins_pre) BETWEEN 95 AND 100 THEN 'HIGH'
	END AS SPINS_PRE_SEGMENT,

	CASE 
		WHEN MAX(tokens_pre) BETWEEN 0 AND 64 THEN 'LOW'
		WHEN MAX(tokens_pre) BETWEEN 65 AND 94 THEN 'MID'
		WHEN MAX(tokens_pre) BETWEEN 95 AND 100 THEN 'HIGH'
	END AS TOKENS_PRE_SEGMENT

FROM distributions	
GROUP BY 1)

SELECT
	DISTINCT
	swrve_user_id,
	SPINS_SEGMENT,
	TOKENS_SEGMENT
FROM final

