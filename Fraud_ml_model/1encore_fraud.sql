{{ config(
    materialized='table',
    alias ='encore_fraud'
) }}

-- fraud cases in encore

SELECT *
FROM hbg_bi.sandbox_analytics.fraud_bk_fy24 truef -- fraud cases labelled from fraud team
-- match fraud cases with production in encore
LEFT JOIN (SELECT * 
			FROM {{source('api_analytics','encore_raw_follow_the_booking')}}
			WHERE booking_timestamp::DATE BETWEEN ('2023-08-01') and ('2024-10-30') 
			QUALIFY ROW_NUMBER() OVER (PARTITION BY bookingreference ORDER BY booking_timestamp asc) =1) lh
ON truef.LOCALIZADOR = lh.BOOKINGREFERENCE
-- of those that are not in encore, we keep only Bedbank and exclude Activities
LEFT JOIN (SELECT DISTINCT REPLACE(booking_id,'|','-') AS booking_id FROM hbg_bi.COMMUNITY_ANALYTICS.REPORTING WHERE gross_flag='Gross' AND business_unit = 'B') b
ON localizador = b.booking_id
WHERE FEC_CREACION BETWEEN ('2023-08-01') and ('2024-10-30')
AND FRAUD_TYPE != 'Debtor Client' 
AND (bookingreference IS NOT NULL 
OR (bookingreference IS NULL AND b.booking_id IS NOT NULL))
AND bookingreference IS NOT NULL -- 28 bookings NOT in encore (12 Offline and 16 web) (excluded by the moment)