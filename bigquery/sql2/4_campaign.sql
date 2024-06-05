

create or replace table marketing_final.campaign


as
SELECT 
j1.id as campaignId,
j1.name as campaignName,
j1.activity_id as ActivityID,
j1.Activity_Name as ActivityName
FROM marketing_prep.JourneyPrep j1
where not exists (select 1 from marketing_prep.campaign_full j2
                where j1.id = j2.campaignId
                and j1.activity_id = j2.ActivityID
                )


;
-- RETROFIT vanno in delta su 3 giorni
insert marketing_prep.campaign_full
select 
campaignId,
campaignName,
ActivityID,
ActivityName,
current_timestamp as INSERT_TIMESTAMP
from
marketing_final.campaign
 
;
 
 
 