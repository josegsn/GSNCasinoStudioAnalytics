WITH historical_data AS (
SELECT
	synthetic_id,
	date('{{ ds }}') - 1 - MAX(event_day) AS last_purchase_days,
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

, users AS (
SELECT
	DISTINCT swrve_user_id,
	CASE 
		WHEN revenue_lt IS NULL THEN 'A'
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
	AS STORE_SEGMENT


FROM gsnmobile.events_dau  a
FULL OUTER JOIN historical_data b
	USING(synthetic_id)
JOIN gsnmobile.swrve_casino_mapping y
	ON b.mesmo_id = y.mesmoid
WHERE a.app = 'GSN Casino'
	AND a.event_day BETWEEN date('{{ ds }}') - 90 AND date('{{ ds }}') 
	)

SELECT 
	DISTINCT
	swrve_user_id,
	STORE_SEGMENT
FROM users

