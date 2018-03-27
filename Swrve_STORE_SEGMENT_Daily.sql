
WITH historical_data AS (
SELECT
	synthetic_id,
	date('{{ ds }}') - 1 - MAX(event_day) AS last_purchase_days,
	SUM(CASE WHEN event_day BETWEEN date('{{ ds }}') - 46 AND date('{{ ds }}') - 2 THEN 1 ELSE 0 END) AS transactions_46,
	SUM(CASE WHEN event_day BETWEEN date('{{ ds }}') - 46 AND date('{{ ds }}') - 2 THEN amount_paid_usd ELSE 0 END) AS revenue_46,
	SUM(CASE WHEN event_day BETWEEN date('{{ ds }}') - 45 AND date('{{ ds }}') - 1 THEN 1 ELSE 0 END) AS transactions_45,
	SUM(CASE WHEN event_day BETWEEN date('{{ ds }}') - 45 AND date('{{ ds }}') - 1 THEN amount_paid_usd ELSE 0 END) AS revenue_45,
	SUM(amount_paid_usd) AS revenue_lt,
	COUNT(*) AS transactions_lt
FROM gsnmobile.events_payments
WHERE app = 'GSN Casino'
GROUP BY 1)

, users AS (
SELECT
	DISTINCT swrve_user_id,
	revenue_45,
	revenue_46,
	last_purchase_days,
	CASE 
		WHEN revenue_lt < 5 THEN 'A' 
                 WHEN revenue_lt  > 0 AND revenue_45 = 0 AND revenue_lt < 5 THEN 'A'
                 WHEN revenue_lt  > 0 AND revenue_45 = 0 AND revenue_lt >= 5 THEN 'B'
	  	 WHEN  transactions_lt <= 1 THEN 'A'
	  	 WHEN  transactions_lt > 1 AND revenue_45/transactions_45 <= 4 THEN 'B'
	  	 WHEN  transactions_lt > 1 AND revenue_45/transactions_45 > 4 AND revenue_45/transactions_45 <= 8 THEN 'C'
	  	 WHEN  transactions_lt > 1 AND revenue_45/transactions_45 > 8 AND revenue_45/transactions_45 <= 15 THEN 'D'
	  	 WHEN  transactions_lt > 1 AND revenue_45/transactions_45 > 15 AND revenue_45/transactions_45 <= 35 THEN 'E'
	  	 WHEN  transactions_lt > 1 AND revenue_45/transactions_45 > 35 AND revenue_45/transactions_45 <= 74 THEN 'F'
	  	 WHEN  transactions_lt > 1 AND revenue_45/transactions_45 > 75 THEN 'G'
		END 
	AS STORE_SEGMENT,
	CASE 
		WHEN revenue_lt < 5 THEN 'A' 
                 WHEN revenue_lt  > 0 AND revenue_46 = 0 AND revenue_lt < 5 THEN 'A'
                 WHEN revenue_lt  > 0 AND revenue_46 = 0 AND revenue_lt >= 5 THEN 'B'
	  	 WHEN  transactions_lt <= 1 THEN 'A'
	  	 WHEN  transactions_lt > 1 AND revenue_46/transactions_46 <= 4 THEN 'B'
	  	 WHEN  transactions_lt > 1 AND revenue_46/transactions_46 > 4 AND revenue_46/transactions_46 <= 8 THEN 'C'
	  	 WHEN  transactions_lt > 1 AND revenue_46/transactions_46 > 8 AND revenue_46/transactions_46 <= 15 THEN 'D'
	  	 WHEN  transactions_lt > 1 AND revenue_46/transactions_46 > 15 AND revenue_46/transactions_46 <= 35 THEN 'E'
	  	 WHEN  transactions_lt > 1 AND revenue_46/transactions_46 > 35 AND revenue_46/transactions_46 <= 74 THEN 'F'
	  	 WHEN  transactions_lt > 1 AND revenue_46/transactions_46 > 75 THEN 'G'
		END 
	AS STORE_SEGMENT_PREV


FROM historical_data a
JOIN gsnmobile.dim_device_mesmoids m 
	USING(synthetic_id)
JOIN gsnmobile.swrve_casino_mapping y
	ON m.mesmo_id = y.mesmoid
WHERE m.app_name = 'GSN Casino'
)

SELECT 
	DISTINCT
	swrve_user_id,
	STORE_SEGMENT
FROM users
WHERE ((revenue_45 > 0 AND revenue_46 = 0)
		OR (STORE_SEGMENT_PREV <> STORE_SEGMENT)
		OR (last_purchase_days = 46))

;

-- 0-60 / 60 - 95 / > 95
WITH values_user AS(
SELECT
	synthetic_id,
	
	CASE 
		WHEN IFNULL(COUNT(DISTINCT CASE 
									WHEN event_day BETWEEN CURRENT_DATE - 7  AND CURRENT_DATE - 1  
									THEN synthetic_id 
									ELSE NULL END),0) = 0 
	    THEN 0 
	    ELSE 
			SUM(CASE 
					WHEN event_day BETWEEN CURRENT_DATE - 7  AND CURRENT_DATE - 1 
			    				AND event_name = 'adjustTokenBalance' 
			    				AND attr17*1 < 0 
					THEN 1 
					ELSE 0 END)/
			COUNT(DISTINCT CASE 
				WHEN event_day BETWEEN CURRENT_DATE - 7  AND CURRENT_DATE - 1  
				THEN synthetic_id 
				ELSE NULL END)  END AS spins,
						
	CASE 
		WHEN IFNULL(COUNT(DISTINCT CASE 
									WHEN event_day BETWEEN CURRENT_DATE - 8  AND CURRENT_DATE - 2  
									THEN synthetic_id 
									ELSE NULL END),0) = 0 
	    THEN 0 
	    ELSE 
			SUM(CASE 
					WHEN event_day BETWEEN CURRENT_DATE - 8  AND CURRENT_DATE - 2 
			    				AND event_name = 'adjustTokenBalance' 
			    				AND attr17*1 < 0 
					THEN 1 
					ELSE 0 END)/
			COUNT(DISTINCT CASE 
				WHEN event_day BETWEEN CURRENT_DATE - 8  AND CURRENT_DATE - 2  
				THEN synthetic_id 
				ELSE NULL END)  END AS spins_pre,			

	
	CASE 
		WHEN IFNULL(COUNT(DISTINCT CASE 
									WHEN event_day BETWEEN CURRENT_DATE - 7  AND CURRENT_DATE - 1  
									THEN synthetic_id 
									ELSE NULL END),0) = 0 
	    THEN 0 
	    ELSE 
			SUM(CASE 
					WHEN event_day BETWEEN CURRENT_DATE - 7  AND CURRENT_DATE - 1 
			    				AND event_name = 'adjustTokenBalance' 
			    				AND attr17*1 < 0 
					THEN attr17*-1 
					ELSE 0 END)/
			COUNT(DISTINCT CASE 
				WHEN event_day BETWEEN CURRENT_DATE - 7  AND CURRENT_DATE - 1  
				THEN synthetic_id 
				ELSE NULL END)  END AS tokens,
						
	CASE 
		WHEN IFNULL(COUNT(DISTINCT CASE 
									WHEN event_day BETWEEN CURRENT_DATE - 8  AND CURRENT_DATE - 2  
									THEN synthetic_id 
									ELSE NULL END),0) = 0 
	    THEN 0 
	    ELSE 
			SUM(CASE 
					WHEN event_day BETWEEN CURRENT_DATE - 8  AND CURRENT_DATE - 2 
			    				AND event_name = 'adjustTokenBalance' 
			    				AND attr17*1 < 0 
					THEN attr17*-1 
					ELSE 0 END)/
			COUNT(DISTINCT CASE 
				WHEN event_day BETWEEN CURRENT_DATE - 8  AND CURRENT_DATE - 2  
				THEN synthetic_id 
				ELSE NULL END)  END AS tokens_pre
FROM gsnmobile.events
WHERE event_name IN ('adjustTokenBalance', 'getConfiguration')
	AND event_day BETWEEN CURRENT_DATE - 8 AND CURRENT_DATE - 1
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


LIMIT 1000