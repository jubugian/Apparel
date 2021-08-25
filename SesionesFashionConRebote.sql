WITH 
dateFrom AS (SELECT DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)) 
--, dateTo AS (SELECT DATE_SUB(CURRENT_DATE(), INTERVAL 3 DAY)) 
, ga_sessions AS (

      SELECT date
      , fullVisitorId
      , visitStartTime
      , site
      , hits
      , totals
      FROM `calm-mariner-105612.ALL_GA360.ga_sessions`
      WHERE date = (SELECT * FROM dateFrom)
      AND businessUnit = 'ML'
      AND site IN ('MLA','MLM','MLB','MCO')      
       )

, sqlBase AS (
    SELECT
      ga_sessions.date AS Fecha
    , fullVisitorId
    , visitStartTime
    , site AS siteID
    , ITE.ITE_ITEM_ID AS itemID
    , ITE.BRAND_ITEM AS Brand
    , CASE
          WHEN (hits.type = 'PAGE') THEN hits.page.pagePath
          WHEN (hits.type = 'APPVIEW') THEN hits.appInfo.screenName
          WHEN ((hits.type = 'EVENT' OR hits.type = 'SOCIAL') AND hits.appInfo.appName IS NULL) THEN hits.page.pagePath
          WHEN ((hits.type = 'EVENT' OR hits.type = 'SOCIAL') AND hits.appInfo.appName IS NOT NULL) THEN hits.appInfo.screenName
          ELSE null 
          END  AS pagePath
    , CASE WHEN hits.appInfo.appName LIKE 'Mercado%Li%iOS%'THEN 'App iOS' 
            WHEN hits.appInfo.appName LIKE 'Mercado%Li%Android%' THEN 'App Android'
            WHEN ( SELECT value FROM hits.customDimensions WHERE index=1 ) IN ('desktop', 'tablet', 'UNKNOWN', 'forced_desktop') THEN 'Web Desktop'
            WHEN ( SELECT value FROM hits.customDimensions WHERE index=1 ) IN ('mobile', 'webview', 'WEBVIEW', 'MOBILE')  THEN 'Web Mobile'
        ELSE 'Web Desktop' END AS Platform 
    , ( SELECT value FROM hits.customDimensions WHERE index=86 )   AS categoryDomain
    , totals
    , COALESCE(hits.isEntrance,FALSE) = TRUE AS isEntrance
    , hits.isExit AS isExit
    , hits.hitNumber As hitNumber
    , hits.type As type
    , CASE
          WHEN hits.appInfo.appName IS NOT NULL THEN (totals.hits = 1 AND COALESCE(hits.isExit,FALSE))
          ELSE NOT (totals.bounces is NULL)
      END  AS  isBounce
FROM ga_sessions, UNNEST(ga_sessions.hits) as hits
    JOIN `meli-bi-data.EXPLOTACION.STAGING_DM_ITEMS_APPAREL` ITE
    ON ( SELECT value FROM hits.customDimensions WHERE index=49 ) = CONCAT(ITE.SIT_SITE_ID, ITE.ITE_ITEM_ID)
WHERE ((CASE
          WHEN (hits.type = 'PAGE') THEN hits.page.pagePath
          WHEN (hits.type = 'APPVIEW') THEN hits.appInfo.screenName
          WHEN ((hits.type = 'EVENT' OR hits.type = 'SOCIAL') AND hits.appInfo.appName IS NULL) THEN hits.page.pagePath
          WHEN ((hits.type = 'EVENT' OR hits.type = 'SOCIAL') AND hits.appInfo.appName IS NOT NULL) THEN hits.appInfo.screenName
          ELSE null END) LIKE '%/PDP/%' OR 
    (CASE
          WHEN (hits.type = 'PAGE') THEN hits.page.pagePath
          WHEN (hits.type = 'APPVIEW') THEN hits.appInfo.screenName
          WHEN ((hits.type = 'EVENT' OR hits.type = 'SOCIAL') AND hits.appInfo.appName IS NULL) THEN hits.page.pagePath
          WHEN ((hits.type = 'EVENT' OR hits.type = 'SOCIAL') AND hits.appInfo.appName IS NOT NULL) THEN hits.appInfo.screenName
          ELSE null END) LIKE '%/VIP/ITEM/MAIN/%')
    AND IF(hits.page.pageTitle IS NOT NULL, REGEXP_CONTAINS(hits.page.pageTitle, r'^app-|^old-world$|^nodejs-|^salesforce$'),IF(hits.appInfo.appName IS NOT NULL,TRUE,FALSE))
    )


SELECT Fecha  
, siteID  
, itemID  
, Brand 
, pagePath  
, Platform  
, categoryDomain  
, COUNT(DISTINCT CONCAT(fullVisitorId,visitStartTime)) AS totals_sessions 
, COUNT(DISTINCT fullVisitorId) AS usuarios 
, ROUND(COALESCE(CAST( ( SUM(DISTINCT (CAST(ROUND(COALESCE( totals.newVisits  ,0)*(1/1000*1.0), 9) AS NUMERIC) + (cast(cast(concat('0x', substr(to_hex(md5(CAST( CONCAT(fullVisitorId,visitStartTime)   AS STRING))), 1, 15)) as int64) as numeric) * 4294967296 + cast(cast(concat('0x', substr(to_hex(md5(CAST( CONCAT(fullVisitorId,visitStartTime)   AS STRING))), 16, 8)) as int64) as numeric)) * 0.000000001 )) - SUM(DISTINCT (cast(cast(concat('0x', substr(to_hex(md5(CAST( CONCAT(fullVisitorId,visitStartTime)  AS STRING))), 1, 15)) as int64) as numeric) * 4294967296 + cast(cast(concat('0x', substr(to_hex(md5(CAST( CONCAT(fullVisitorId,visitStartTime)   AS STRING))), 16, 8)) as int64) as numeric)) * 0.000000001) )  / (1/1000*1.0) AS FLOAT64), 0), 6) AS NuevosUsuarios  

, ROUND((COUNT(DISTINCT IF (isEntrance AND isBounce, CONCAT(fullVisitorId, CAST(visitStartTime AS STRING)), NULL)) / NULLIF(COUNT(DISTINCT CONCAT(fullVisitorId, CAST(visitStartTime AS STRING))),0))*100,2) AS BounceRate --se calcula sobre todas las sesiones que visitaron la página, sea o no sea landing  

, COUNT(DISTINCT IF (isEntrance AND isBounce, CONCAT(fullVisitorId, CAST(visitStartTime AS STRING)), NULL)) sesiones_c_rebote_p_rebote
, NULLIF(COUNT(DISTINCT CONCAT(fullVisitorId, CAST(visitStartTime AS STRING))),0) totalSessions_para_rebote

, COUNT(DISTINCT CASE WHEN isExit THEN ( CONCAT(CONCAT(fullVisitorId,visitStartTime),'|',FORMAT('%05d',hitNumber))  )  ELSE NULL END) / NULLIF(COUNT(DISTINCT CASE WHEN (type IN ('PAGE', 'APPVIEW')) THEN CONCAT(CONCAT(fullVisitorId,visitStartTime),'|',FORMAT('%05d',hitNumber))  ELSE NULL END),0) AS exitRate
FROM sqlBase  
GROUP BY 1, 2, 3, 4, 5, 6, 7
ORDER BY totals_sessions DESC 