{{ config(
    materialized='table',
    alias ='fct_fraud_ml_preprocessing_gb'
) }}

-- whitelist

WITH encore_fraudflag_row AS (
    SELECT *
    , ROW_NUMBER() OVER (PARTITION BY client_id , client_ip ORDER BY booking_timestamp )-1 AS num_booking_prior
    , ROW_NUMBER() OVER (PARTITION BY client_id  ORDER BY booking_timestamp )-1 AS num_booking_client
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
    , ZEROIFNULL(total_ko_web) AS total_ko_web
    , COALESCE(de.top_fraud_destination_flag,'0') AS top_fraud_destination_flag
    , ZEROIFNULL(h0.adr) AS adr
    -- , ZEROIFNULL(sts.hotel_cost_max) AS hotel_cost_max
    -- , ZEROIFNULL(sts.hotel_cost_max_year) AS hotel_cost_max_year
    -- , ZEROIFNULL(sts.cluster_cost_max) AS cluster_cost_max
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
    FROM {{source('hbg_bi_community_analytics','reporting')}}
    WHERE reference_date>= '2023-01-01' AND gross_flag = 'Gross' GROUP BY all) h0
ON tr.bookingreference=h0.booking_id 
LEFT JOIN 
	(SELECT DISTINCT hotel_code, to_char(arrival_month) AS arrival_month, hotel_cost_max,hotel_cost_avg,hotel_cost_dev,hotel_cost_max_year, hotel_cost_avg_year, hotel_cost_dev_year, cluster_cost_max, cluster_cost_avg, cluster_cost_dev 
	FROM {{source('hbg_bi_community_analytics','fct_fraud_trading_statistics')}}
	) sts
ON tr.hotel_code=sts.hotel_code
AND to_char(checkin_month)=sts.arrival_month
LEFT JOIN (SELECT destination_id
                , CASE WHEN country_code IN ('AE','ID','MY','TH') THEN True ELSE False END AS top_fraud_destination_flag
            FROM {{source('prod_aqua','DIM_DESTINATIONS')}}
            WHERE company = 'HBD') de
ON destination_code = de.destination_id
WHERE 
(num_booking_prior <=20 
OR different_days_ip_id <=20)
--OR num,-2,booking_timestamp))
--AND _booking_client <=10 
--OR first_user_appear <= DATEADD(MONTH
--ADR >0 --there're lots of bookings with negative_om because of compensations, etc.
