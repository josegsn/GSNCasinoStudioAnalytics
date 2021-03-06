-- Getting 45 days data (segment) and the data of from the 45 days before yesterday (prev segment data-point)

WITH historical_data AS (
SELECT
	synthetic_id,
	date('{{ ds }}') - MAX(event_day) AS last_purchase_days,
	MAX(user_id*1) AS mesmo_id,
	SUM(CASE WHEN event_day BETWEEN date('{{ ds }}') - 46 AND date('{{ ds }}') - 1 THEN 1 ELSE 0 END) AS transactions_46,
	SUM(CASE WHEN event_day BETWEEN date('{{ ds }}') - 46 AND date('{{ ds }}') - 1 THEN amount_paid_usd ELSE 0 END) AS revenue_46,
	SUM(CASE WHEN event_day BETWEEN date('{{ ds }}') - 45 AND date('{{ ds }}') - 0 THEN 1 ELSE 0 END) AS transactions_45,
	SUM(CASE WHEN event_day BETWEEN date('{{ ds }}') - 45 AND date('{{ ds }}') - 0 THEN amount_paid_usd ELSE 0 END) AS revenue_45,
	SUM(amount_paid_usd) AS revenue_lt,
	COUNT(*) AS transactions_lt
FROM gsnmobile.events_payments
WHERE app = 'GSN Casino'
GROUP BY 1) 

-- Calculating current segment and previous segment

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
JOIN gsnmobile.swrve_casino_mapping y
	ON a.mesmo_id = y.mesmoid
)

SELECT 
	DISTINCT
	swrve_user_id,
	STORE_SEGMENT
FROM users

WHERE ((revenue_45 > 0 AND revenue_46 = 0) -- New spender or re-activated iddle spender
		OR (STORE_SEGMENT_PREV <> STORE_SEGMENT) -- Change in store segnent
		OR (last_purchase_days = 46)) -- Active spender becoming iddle spender

