CREATE OR REPLACE PROCEDURE `ETL20A.CI_CAMPAIGN_RETROFIT_proc`(to_date STRING)
OPTIONS (strict_mode=false)
BEGIN
 
DELETE FROM ETL20A.CI_CAMPAIGN_RETROFIT_FULL
where date(INSERT_TIMESTAMP) < date_sub(parse_date("%Y%m%d", to_date), interval 4 DAY);
 
CREATE OR REPLACE TABLE `ETL20A.CI_CAMPAIGN_RETROFIT`
as
with t as (SELECT 
JourneyID as CAMPAIGN_CD,
JourneyName as CAMPAIGN_DESC,
JourneyName as CAMPAIGN_NM,
VersionID as CAMPAIGN_SK
FROM ETL20A.Journey j1
where  (j1.JourneyStatus='Running' or j1.JourneyStatus='Finishing' or j1.JourneyStatus='Stopped' or j1.JourneyStatus='Paused')
and not exists (select 1 from ETL20A.CI_CAMPAIGN_RETROFIT_FULL j2
                where j1.JourneyID = j2.CAMPAIGN_CD
                and j1.VersionID = j2.CAMPAIGN_SK
                and date(j2.INSERT_TIMESTAMP) != parse_date("%Y%m%d", to_date) -- TODO replace this. Always produce in the output the same things if we run automations two times in the same day (for now)
                )) select * from t qualify row_number() over(partition by 
                          t.CAMPAIGN_SK
                         ) = 1;

 
-- RETROFIT vanno in delta su 3 giorni
insert `ETL20A.CI_CAMPAIGN_RETROFIT_FULL`  
select 
CAMPAIGN_CD,
CAMPAIGN_DESC,
CAMPAIGN_NM,
CAMPAIGN_SK,
timestamp(parse_date("%Y%m%d", to_date)) as INSERT_TIMESTAMP
from
ETL20A.CI_CAMPAIGN_RETROFIT
 
;
 
 
 
 
END;