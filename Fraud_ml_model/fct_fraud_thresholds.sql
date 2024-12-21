-- This table is part of the FRAUD project. 

-- This table is the first step on fraud detection algorithm.
-- Its sole purpose is to define a filtering of those bookings that can not be fraudulent. When a booking
-- pass the filter it means that can potentially mean there is an error.

-- Filtra, nos quedamos con las bookings que pueden ser fraude: las que tienen ip usado <20 veces con un mismo cliente, 
-- las que tienen un ip con el que se han logeado <20 dias diferentes y las que son de un cliente
-- han tenido algún KO last 24h, o las que han tenido algun autenticationko last 24h

WITH count_client_ip AS  -- ips used less than 20 times by the same client
(
    SELECT DISTINCT  
        cl.client_id,
        client_ip,
        COUNT(*)
    FROM {{source('api_analytics','encore_raw_follow_the_booking')}} raw
    LEFT JOIN (SELECT DISTINCT 
                    client_xml_user,
                    COALESCE(client_main_code,client_id) client_id,
                FROM {{source('api_analytics','encore_masterdata_clients')}} ) cl
    ON LOWER(raw.apikey) = LOWER(cl.client_xml_user)
    WHERE booking_timestamp >= DATEADD(month, -6, current_timestamp)
    AND client_ip IS NOT NULL
    GROUP BY 1,2
    HAVING count(*)<=20
),
count_days_client_ip AS -- ips used (loginok) in less than 20 different days by the same client (web)
(
    SELECT 
        resultado,
        ip,
        COUNT(DISTINCT CASE WHEN error='0' THEN event_timestamp::DATE END) c
    FROM {{ref('fct_evolution_audit_confirm_GEOIP_data')}}
    WHERE event_timestamp >= DATEADD(month, -6, current_timestamp)
    GROUP BY 1,2
    HAVING c<=20
), 
count_days_client_ip2 AS -- ips used (autenticationok) in less than 20 different days by the same client (api)
(
    SELECT 
        apikey,
        COUNT(DISTINCT CASE WHEN response_code = '401' THEN NULL ELSE start_hour::DATE END) c
    FROM {{source('api_analytics','encore_tyk_errors_stats')}}
    GROUP BY 1
    HAVING c<=20
),
last_hour AS (
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
    preconfirmed_confirmed_booking
FROM {{source('api_analytics','encore_raw_follow_the_booking')}} lh
-- select only last hour information
WHERE etl_id =  (SELECT etl_id FROM {{source('api_analytics','encore_raw_follow_the_booking')}} ORDER BY etl_id DESC LIMIT 1)
AND bookingreference IS NOT NULL 
-- just want the first appearance of the booking
QUALIFY ROW_NUMBER() OVER (PARTITION BY bookingreference ORDER BY booking_timestamp asc) =1)
, total_ko AS (
SELECT last_hour.*, cl.client_id
    , case when channel = 'evolution' then COALESCE((SELECT count(*)
			FROM hbg_bi.community_analytics.fct_evolution_audit_confirm_geoip_data lgweb
			WHERE lgweb.resultado =cl.client_id::integer::string
			AND ERROR='1'
			AND lgweb.event_timestamp  BETWEEN DATEADD(hour,-24,booking_timestamp) AND booking_timestamp),
			0) 
            else NULL end  AS total_ko_web
    , case when channel = 'XML 3' then COALESCE((SELECT sum(lg.hits)
			FROM HBG_API_ANALYTICS.COMMUNITY_SOURCE.ENCORE_TYK_ERRORS_STATS lg
			WHERE lg.apikey =credential
			AND response_code='401'
			AND lg.start_hour BETWEEN DATEADD(hour,-24,booking_timestamp) AND booking_timestamp),
			0)
            else null end  AS total_ko_api
FROM last_hour
LEFT JOIN  
	(SELECT DISTINCT client_xml_user, COALESCE(client_main_code,client_id) client_id 
    FROM  "HBG_API_ANALYTICS"."COMMUNITY_SOURCE"."ENCORE_MASTERDATA_CLIENTS" ) CL
ON LOWER(last_hour.credential) = LOWER(cl.client_xml_user))

SELECT total_ko.*, 
    count_client_ip.client_ip as client_ip_2,
    count_days_client_ip.ip,
    count_days_client_ip2.apikey
FROM total_ko
LEFT JOIN count_client_ip
ON total_ko.client_ip =count_client_ip.client_ip 
AND total_ko.client_id::integer::string =count_client_ip.client_id::integer::string
LEFT JOIN count_days_client_ip
ON total_ko.client_ip =count_days_client_ip.ip 
AND total_ko.client_id::integer::string = count_days_client_ip.resultado
AND total_ko.channel = 'evolution'
LEFT JOIN count_days_client_ip2
ON total_ko.credential =count_days_client_ip2.apikey
AND total_ko.channel ='XML 3'
-- it can have any KO in last24h, IP used almost 20 times by the same client,
-- IP used almost 20 days by the same client in a loginok (web), apikey used almost 20 days by the same client in an autenticationok 
WHERE count_client_ip.CLIENT_IP IS NOT NULL OR count_days_client_ip.ip IS NOT NULL OR count_days_client_ip2.apikey IS NOT NULL
OR TOTAL_KO_WEB >0 OR TOTAL_KO_API>0