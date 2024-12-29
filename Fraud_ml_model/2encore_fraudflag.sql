{{ config(
    materialized='table',
    alias='encore_fraudflag'
) }}

-- fraud and no fraud cases in encore
WITH first_user_appearance AS (
SELECT RQ_BOOKING:body:"creationUser"::STRING AS creation_user ,booking_timestamp 
FROM {{source('api_analytics','encore_raw_follow_the_booking')}}
WHERE creation_user IS NOT NULL AND bookingreference IS NOT NULL
QUALIFY ROW_NUMBER() OVER (PARTITION BY creation_user ORDER BY booking_timestamp )-1=0
ORDER BY 1
)
, last_hour AS (
SELECT DISTINCT 
    bookingreference,
    booking_timestamp,
    rate_class_booking,
    product_type,
    channel,
    lh.apikey AS credential,
    COALESCE(RQ_BOOKING:body:"xapplication"::STRING,channel) AS application,
    RQ_BOOKING:body:"creationUser"::string AS creation_user,
    client_ip,
    CASE WHEN checkrate_timestamp IS NULL THEN 'No Check Rate' ELSE '' END AS no_check_rate_flag,
    client_ip_country,
    source_market,
    destination_code,
    checkin,
    MONTH(checkin::DATE) AS checkin_month	,
    checkout,
    lead_time_days,
    length_of_stay,
    lh.hotel_code,
    search_type,
    rs_booking:body:holder:"name"::string holder_name,
    rs_booking:body:holder:"surname"::string holder_surname,
    rs_booking:body:"status"::string status,
    rs_booking:booking:hotel:"currency"::string curr,
    rs_booking:booking:"totalSellingRate"::string total_Price,
    rs_checkrate:hotel:"paymentDataRequired"::string payment_data_required_flag,
    rs_checkrate:hotel:rooms:rates:"paymentType"::string payment_type,
    rs_checkrate:hotel:rooms:rates:"roomPrice"::string room_price,
    client_reference,
    booking_modification_flag,
    interface,
    preconfirmed_confirmed_booking,
    ecb.bookingamount,
    ecb.currency,
    ecb.paymenttype,
    ecb.username,
    cl.client_id ,
    CASE WHEN localizador IS NULL THEN FALSE
    ELSE TRUE END AS is_fraud
FROM {{source('api_analytics','encore_raw_follow_the_booking')}} lh
LEFT JOIN (SELECT DISTINCT seqbooking, ttoo, username, bookingamount, currency, paymenttype
            FROM {{source('api_analytics','evolution_audit_confirm_booking_operation_data_v')}}
            WHERE bookingoperation = 'Booking') ecb
ON lh.bookingreference = ecb.seqbooking
LEFT JOIN 
	(SELECT DISTINCT localizador FROM {{ref('1encore_fraud')}}) frtrue 
ON bookingreference=frtrue.localizador
LEFT JOIN (SELECT DISTINCT 
                    client_xml_user,
                    COALESCE(client_main_code,client_id) client_id,
                FROM {{source('api_analytics','encore_masterdata_clients')}} ) cl --client_id
ON LOWER(credential) = LOWER(cl.client_xml_user)
WHERE booking_timestamp::DATE between ('2023-08-01') and ('2024-10-30') -- dates where we have fraud cases labelled
AND bookingreference IS NOT NULL 
--AND  booking_modification_flag!='S' -- not a modification, because it causes lead_time very negative
--AND  booking_modification_flag!='Y'
AND lead_time_days <= 302 -- maximum value of the fraud cases
AND (application='evolution' OR localizador IS NOT NULL) -- only web but all from fraud
-- just want the first appearance of the booking, without modifications
QUALIFY ROW_NUMBER() OVER (PARTITION BY bookingreference ORDER BY booking_timestamp asc) =1
)
, diff_days_ipid AS(
    SELECT  resultado, ip ,event_timestamp 
    ,DENSE_RANK() OVER ( PARTITION BY resultado, ip ORDER BY event_timestamp ::DATE  )-1 AS different_days_ip_id
    FROM {{source('hbg_bi_community_analytics','fct_evolution_audit_confirm_geoip_data')}}
    WHERE ip IS NOT NULL
    AND error='0'
)
SELECT last_hour.*
, COALESCE((SELECT count(*)
			FROM {{source('hbg_bi_community_analytics','fct_evolution_audit_confirm_geoip_data')}} lgweb
			WHERE lgweb.resultado IS NOT NULL
            AND lgweb.resultado = last_hour.client_id::integer::string
			AND error='1'
			AND lgweb.event_timestamp  BETWEEN DATEADD(hour,-24,last_hour.booking_timestamp) AND last_hour.booking_timestamp),
			0)   AS total_ko_web
, fa.booking_timestamp AS first_user_appear 
, (select max(different_days_ip_id) 
    from diff_days_ipid dd 
    where dd.resultado = last_hour.client_id::integer::string
    and dd.ip = last_hour.client_ip 
    and dd.event_timestamp<=last_hour.booking_timestamp) as different_days_ip_id
FROM last_hour
LEFT JOIN first_user_appearance fa
on last_hour.creation_user = fa.creation_user
