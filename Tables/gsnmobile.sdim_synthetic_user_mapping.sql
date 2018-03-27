DROP TABLE IF EXISTS gsnmobile.dim_synthetic_user_mapping_90d;

CREATE TABLE  gsnmobile.dim_synthetic_user_mapping_90d AS (

SELECT
	synthetic_id,
	user_id
FROM (
		SELECT
			*,
			RANK() OVER(PARTITION BY user_id ORDER BY days DESC) AS rank
		FROM (
				SELECT
					user_id,
					synthetic_id,
					COUNT(DISTINCT event_day) AS days
				FROM gsnmobile.events_dau
				WHERE app = 'GSN Casino'
					AND event_day BETWEEN CURRENT_DATE - 90 AND CURRENT_DATE - 1
					AND user_id <> 'NULL'
				GROUP BY 1,2
				) a
	 ) b

WHERE rank = 1 

)

