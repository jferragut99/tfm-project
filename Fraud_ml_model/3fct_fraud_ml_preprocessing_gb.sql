{{ config(
    materialized='table',
    alias ='fct_fraud_ml_preprocessing_gb'
) }}

-- take bookings that have <20 appearances

WITH encore_fraudflag_row AS (
SELECT *
, row_number() OVER (PARTITION BY CLIENT_ID , CLIENT_IP ORDER BY BOOKING_TIMESTAMP )-1 AS num_booking_prior
--, lag(booking_timestamp) over(PARTITION BY CLIENT_ID , CLIENT_IP ORDER BY BOOKING_TIMESTAMP) AS BOOKING_TIMESTAMP_prev
FROM {{ref('2encore_fraudflag')}}
)
SELECT
    bookingreference 
    ,booking_timestamp
    , ZEROIFNULL(lead_time_days) AS lead_time_days
    , ZEROIFNULL(length_of_stay) AS length_of_stay
    , COALESCE(paymenttype,'0') AS paymenttype
    , COALESCE(CASE
        WHEN startswith(upper(CLIENT_REFERENCE),'AGY')
				OR startswith(upper(CLIENT_REFERENCE),'BDT')
				OR startswith(upper(CLIENT_REFERENCE),'HOLIDAYS')
				OR startswith(upper(CLIENT_REFERENCE),'ARAS') THEN True
	    ELSE False
        END,'0' ) AS top_fraud_agency_flag
    , COALESCE(channel,'0') AS channel
    , ZEROIFNULL(total_ko_web) AS total_ko_web
    , ZEROIFNULL(total_ko_api) AS total_ko_api
    , COALESCE(de.top_fraud_destination_flag,'0') AS top_fraud_destination_flag
    , ZEROIFNULL(h0.adr) AS adr
    , ZEROIFNULL(sts.hotel_cost_max) AS hotel_cost_max
    , ZEROIFNULL(sts.hotel_cost_max_year) AS hotel_cost_max_year
    , ZEROIFNULL(sts.cluster_cost_max) AS cluster_cost_max
    , ZEROIFNULL((adr - hotel_cost_avg )/ nullif(hotel_cost_avg,0)) AS dicrepancy_CM
    , ZEROIFNULL((adr - hotel_cost_avg_year )/ nullif(hotel_cost_avg_year,0)) AS dicrepancy_CY
    , ZEROIFNULL((adr - cluster_cost_avg )/ nullif(cluster_cost_avg,0)) AS dicrepancy_destcat
    , is_fraud
FROM encore_fraudflag_row tr
LEFT JOIN 
	(SELECT REPLACE(booking_id,'|','-') booking_id, 
    SUM(gross_sales),
    case when sum(rn)=0 then sum(rn_all) 
    else sum(rn) end as rn1,
    DIV0NULL(SUM(gross_sales),rn1) adr 
    FROM hbg_bi.community_analytics.reporting 
    WHERE reference_date>= '2023-01-01' AND gross_flag = 'Gross' GROUP BY all) h0
ON tr.bookingreference=h0.booking_id 
LEFT JOIN 
	(SELECT DISTINCT hotel_code, to_char(arrival_month) AS arrival_month, hotel_cost_max,hotel_cost_avg,hotel_cost_dev,hotel_cost_max_year, hotel_cost_avg_year, hotel_cost_dev_year, cluster_cost_max, cluster_cost_avg, cluster_cost_dev 
	FROM Hbg_bi.community_analytics.fct_fraud_trading_statistics
	) sts
ON tr.hotel_code=sts.hotel_code
AND to_char(checkin_month)=sts.arrival_month
LEFT JOIN (SELECT destination_id
                , CASE WHEN country_code IN ('AE','ID','MY','TH') THEN True ELSE False END AS top_fraud_destination_flag
            FROM prod_hbg.aqua.dim_destinations
            WHERE company = 'HBD') de
ON destination_code = de.destination_id
WHERE 1=1
AND NUM_BOOKING_PRIOR <= 20