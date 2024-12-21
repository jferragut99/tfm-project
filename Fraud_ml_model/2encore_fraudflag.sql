{{ config(
    materialized='table',
    alias='encore_fraudflag'
) }}

-- fraud and no fraud cases in encore
WITH last_hour AS (
SELECT DISTINCT 
    bookingreference,
    booking_timestamp,
    rate_class_booking,
    product_type,
    channel,
    lh.apikey AS credential,
    RQ_BOOKING:body:"xapplication"::string AS application,
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
    cl.CLIENT_ID ,
    CASE WHEN localizador IS NULL THEN FALSE
    ELSE TRUE END AS is_fraud
FROM hbg_api_analytics.community_source.encore_raw_follow_the_booking lh
LEFT JOIN (SELECT DISTINCT seqbooking, ttoo, username, bookingamount, currency, paymenttype
            FROM hbg_api_analytics.community_source.evolution_audit_confirm_booking_operation_data_v
            WHERE bookingoperation = 'Booking') ecb
ON lh.bookingreference = ecb.seqbooking
LEFT JOIN 
	(SELECT DISTINCT localizador FROM {{ref('1encore_fraud')}}) frtrue 
ON bookingreference=frtrue.localizador
LEFT JOIN (SELECT DISTINCT 
                    client_xml_user,
                    COALESCE(client_main_code,client_id) client_id,
                FROM hbg_api_analytics.community_source.encore_masterdata_clients ) cl --client_id
ON LOWER(credential) = LOWER(cl.client_xml_user)
WHERE booking_timestamp::DATE between ('2023-08-01') and ('2024-10-30') -- dates where we have fraud cases labelled
AND bookingreference IS NOT NULL 
AND  booking_modification_flag!='S' -- not a modification, because it causes lead_time very negative
AND  booking_modification_flag!='Y'
AND lead_time_days <= 455 -- maximum value of the fraud cases
-- just want the first appearance of the booking, without modifications
QUALIFY ROW_NUMBER() OVER (PARTITION BY bookingreference ORDER BY booking_timestamp asc) =1
)
SELECT last_hour.*
    , case when channel = 'evolution' or channel = 'evolution_hotelapi' then COALESCE((SELECT count(*)
			FROM hbg_bi.community_analytics.fct_evolution_audit_confirm_geoip_data lgweb
			WHERE lgweb.resultado IS NOT NULL
            AND lgweb.resultado = last_hour.client_id::integer::string
			AND ERROR='1'
			AND lgweb.event_timestamp  BETWEEN DATEADD(hour,-24,booking_timestamp) AND booking_timestamp),
			0) 
            else NULL end  AS total_ko_web
    , case when channel = 'XML 3' then COALESCE((SELECT sum(lg.hits)
			FROM HBG_API_ANALYTICS.COMMUNITY_SOURCE.ENCORE_TYK_ERRORS_STATS lg
			WHERE lg.apikey IS NOT NULL 
            AND lg.apikey =credential
			AND response_code='401'
			AND lg.start_hour BETWEEN DATEADD(hour,-24,booking_timestamp) AND booking_timestamp),
			0)
            else null end  AS total_ko_api
FROM last_hour