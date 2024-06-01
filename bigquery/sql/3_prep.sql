CREATE OR REPLACE PROCEDURE `ETL20A.DataPrep_proc`()
OPTIONS (strict_mode=false)
BEGIN
CREATE OR REPLACE TABLE `ETL20A.JourneyPrep`
as
SELECT
j.*,
ja.ActivityID,
ja.ActivityName,
ja.ActivityType,
ja.JourneyActivityObjectID
FROM ETL20A.Journey j
INNER JOIN ETL20A.JourneyActivity ja on j.VersionID = ja.VersionID
INNER JOIN ETL20A.TRTTable t ON UPPER(ja.ActivityID) = UPPER(t.ActivityID)
where j.JourneyStatus='Running' or j.JourneyStatus='Finishing' or j.JourneyStatus='Stopped' or j.JourneyStatus='Paused'
;
CREATE OR REPLACE TABLE `ETL20A.MobilePushDetailExtractReport_p_failed`
as
SELECT
p.*,
m.MICROESITO,
m.ESITOMC,
"FAIL" as push_type,

from ETL20A.MobilePushDetailExtractReport p
INNER JOIN ETL20A.Microesiti m on upper(m.ESITOMC) = upper("Push Failed")
where upper(p.status) = upper("failed") or upper(p.status) = upper("fail");


CREATE OR REPLACE TABLE `ETL20A.MobilePushDetailExtractReport_p_consegnata`
as
select 
p.*,
m.MICROESITO,
m.ESITOMC,
"SUCC_NO_CLICK" as push_type,

from ETL20A.MobilePushDetailExtractReport p
LEFT OUTER JOIN (select * from ETL20A.ClickReportPush crp 
where crp.SendDate like '20%' and crp.ClickDate like '%2%' and length(crp.ClickDate) <= 24 and length(crp.SendDate) <= 17
) crp  on cast(p.deviceID as string) = crp.deviceID and p.messageName = crp.messageName 
INNER JOIN ETL20A.Microesiti m on m.ESITOMC = "Push Consegnata"
where upper(p.status) = upper("success")
and crp.deviceID is null
; 




CREATE OR REPLACE TABLE `ETL20A.MobilePushDetailExtractReport_click`
as
select 
distinct
p.*,
crp.COD_ESITO as MICROESITO,
"CLICK" as ESITOMC,
"SUCC_CLICK" as push_type,


from ETL20A.MobilePushDetailExtractReport p
INNER JOIN ETL20A.ClickReportPush crp on cast(p.deviceID as string) = crp.deviceID and p.messageName = crp.messageName 
where crp.SendDate like '20%' and crp.ClickDate like '%2%' and length(crp.ClickDate) <= 24 and length(crp.SendDate) <= 17;

CREATE OR REPLACE TABLE `ETL20A.PushSendLog_formatted`
as
select 
*,
FORMAT_DATETIME(
  "%Y%m%d",
DATETIME(TIMESTAMP(PARSE_DATETIME("%Y-%m-%d %H:%M:%E6S", substring(sl.LogDate, 0, length(sl.LogDate)-3)),
 'America/Chicago'), 'Europe/Rome'
 )
)
 as ymd_logDate,
 FORMAT_DATETIME(
  "%Y%m%d%H",
DATETIME(TIMESTAMP(PARSE_DATETIME("%Y-%m-%d %H:%M:%E6S", substring(sl.LogDate, 0, length(sl.LogDate)-3)),
 'America/Chicago'), 'Europe/Rome')
)
as ymdh_logDate,
FORMAT_DATETIME(
  "%Y%m%d %H:%M:%S",
DATETIME(TIMESTAMP(PARSE_DATETIME("%Y-%m-%d %H:%M:%E6S", substring(sl.LogDate, 0, length(sl.LogDate)-3)),
 'America/Chicago'), 'Europe/Rome')
)
as ymd_hms_logDate,

FORMAT_DATETIME(
  "%Y%m%d %H:%M:%E6S",
DATETIME(TIMESTAMP(PARSE_DATETIME("%Y-%m-%d %H:%M:%E6S", substring(sl.LogDate, 0, length(sl.LogDate)-3)),
 'America/Chicago'), 'Europe/Rome')
)
as ymd_hms_ffffff_logDate

from ETL20A.PushSendLog sl
where length(cast(sl.LogDate as string)) > 19

union all

select
*,
FORMAT_DATETIME(
  "%Y%m%d",
DATETIME(TIMESTAMP(PARSE_DATETIME("%Y-%m-%d %H:%M:%S", sl.LogDate),
 'America/Chicago'), 'Europe/Rome')
)
as ymd_logDate,
FORMAT_DATETIME(
  "%Y%m%d%H",
DATETIME(TIMESTAMP(PARSE_DATETIME("%Y-%m-%d %H:%M:%S", sl.LogDate),
 'America/Chicago'), 'Europe/Rome')
)
as ymdh_logDate,
FORMAT_DATETIME(
  "%Y%m%d %H:%M:%S",
DATETIME(TIMESTAMP(PARSE_DATETIME("%Y-%m-%d %H:%M:%S", sl.LogDate),
'America/Chicago'), 'Europe/Rome')
)
as ymd_hms_logDate,
FORMAT_DATETIME(
  "%Y%m%d %H:%M:%E6S",
DATETIME(TIMESTAMP(PARSE_DATETIME("%Y-%m-%d %H:%M:%S", sl.LogDate),
 'America/Chicago'), 'Europe/Rome')
)
as ymd_hms_ffffff_logDate

from ETL20A.PushSendLog sl
where length(cast(sl.LogDate as string)) <= 19;



CREATE OR REPLACE TABLE `ETL20A.SendLog_formatted`
as
select 
*,
CASE
WHEN length(s.LogDate) > 20 THEN FORMAT_DATETIME(
  "%Y%m%d",
DATETIME(TIMESTAMP(PARSE_DATETIME("%Y-%m-%d %H:%M:%E6S", substring(s.LogDate, 0, length(s.LogDate)-3)),
 'America/Chicago'), 'Europe/Rome'
 )
)
ELSE FORMAT_DATETIME(
  "%Y%m%d",
DATETIME(TIMESTAMP(PARSE_DATETIME("%Y-%m-%d %H:%M:%E3S", s.LogDate),
 'America/Chicago')
 , 'Europe/Rome'
 )
)
END as ymd_LogDate,
from ETL20A.SendLog s; 

CREATE OR REPLACE TABLE `ETL20A.Sent_formatted`
as
select 
*,
FORMAT_DATETIME(
  "%Y%m%d",
PARSE_DATETIME("%m/%d/%Y %I:%M:%E3S %p", s.EventDate)
) as ymd_EventDate,
FORMAT_DATETIME(
  "%Y%m%d %H:%M:%S", 
PARSE_DATETIME("%m/%d/%Y %I:%M:%E3S %p", s.EventDate)
) as ymd_hms_EventDate,
FORMAT_DATETIME(
  "%Y%m%d %H:%M:%E6S", 
PARSE_DATETIME("%m/%d/%Y %I:%M:%E3S %p", s.EventDate)
) as ymd_hms_ffffff_EventDate,
FORMAT_DATETIME(
  "%H:%M:%E3S",
PARSE_DATETIME("%m/%d/%Y %I:%M:%E3S %p", s.EventDate)
) as hms_EventDate,

from ETL20A.Sent s; 


CREATE OR REPLACE TABLE `ETL20A.Clicks_formatted`
as
select 
*,
FORMAT_DATETIME(
  "%H:%M:%E3S",
PARSE_DATETIME("%m/%d/%Y %I:%M:%E3S %p", cl.EventDate)
) as hms_EventDate,
FORMAT_DATETIME(
  "%Y%m%d",
PARSE_DATETIME("%m/%d/%Y %I:%M:%E3S %p", cl.EventDate)
) as ymd_EventDate,
FORMAT_DATETIME(
  "%Y%m%d %H:%M:%S",
PARSE_DATETIME("%m/%d/%Y %I:%M:%E3S %p", cl.EventDate)
) as ymd_hms_EventDate,
FORMAT_DATETIME(
  "%Y%m%d %H:%M:%E6S",
PARSE_DATETIME("%m/%d/%Y %I:%M:%E3S %p", cl.EventDate)
) as ymd_hms_ffffff_EventDate

from ETL20A.Clicks cl;




CREATE OR REPLACE TABLE `ETL20A.Opens_formatted`
as
select 
*,
FORMAT_DATETIME(
  "%H:%M:%E3S",
PARSE_DATETIME("%m/%d/%Y %I:%M:%E3S %p", o.EventDate)
) as hms_EventDate,
FORMAT_DATETIME(
  "%Y%m%d",
PARSE_DATETIME("%m/%d/%Y %I:%M:%E3S %p", o.EventDate)
) as ymd_EventDate,
FORMAT_DATETIME(
  "%Y%m%d %H:%M:%S",
PARSE_DATETIME("%m/%d/%Y %I:%M:%E3S %p", o.EventDate)
) as ymd_hms_EventDate,
FORMAT_DATETIME(
  "%Y%m%d %H:%M:%E6S",
PARSE_DATETIME("%m/%d/%Y %I:%M:%E3S %p", o.EventDate)
) as ymd_hms_ffffff_EventDate

from ETL20A.Opens o; 


CREATE OR REPLACE TABLE `ETL20A.Complaints_formatted`
as
select 
*,
FORMAT_DATETIME(
  "%H:%M:%E3S",
PARSE_DATETIME("%m/%d/%Y %I:%M:%E3S %p", co.EventDate)
) as hms_EventDate,
FORMAT_DATETIME(
  "%Y%m%d",
PARSE_DATETIME("%m/%d/%Y %I:%M:%E3S %p", co.EventDate)
) as ymd_EventDate,
FORMAT_DATETIME(
  "%Y%m%d %H:%M:%S",
PARSE_DATETIME("%m/%d/%Y %I:%M:%E3S %p", co.EventDate)
) as ymd_hms_EventDate,
FORMAT_DATETIME(
  "%Y%m%d %H:%M:%E6S",
PARSE_DATETIME("%m/%d/%Y %I:%M:%E3S %p", co.EventDate)
) as ymd_hms_ffffff_EventDate

from ETL20A.Complaints co
;


CREATE OR REPLACE TABLE `ETL20A.Unsubs_formatted`
as
select 
*,
FORMAT_DATETIME(
  "%H:%M:%E3S",
PARSE_DATETIME("%m/%d/%Y %I:%M:%E3S %p", u.EventDate)
) as hms_EventDate,
FORMAT_DATETIME(
  "%Y%m%d",
PARSE_DATETIME("%m/%d/%Y %I:%M:%E3S %p", u.EventDate)
) as ymd_EventDate,
FORMAT_DATETIME(
  "%Y%m%d %H:%M:%S",
PARSE_DATETIME("%m/%d/%Y %I:%M:%E3S %p", u.EventDate)
) as ymd_hms_EventDate,
FORMAT_DATETIME(
  "%Y%m%d %H:%M:%E6S",
PARSE_DATETIME("%m/%d/%Y %I:%M:%E3S %p", u.EventDate)
) as ymd_hms_ffffff_EventDate

from ETL20A.Unsubs u
;


CREATE OR REPLACE TABLE `ETL20A.Bounce_formatted`
as
select 
*,
CASE
WHEN length(b.EventDate) > 20 THEN 
FORMAT_DATETIME(
  "%H:%M:%E3S",
DATETIME(TIMESTAMP(PARSE_DATETIME("%Y-%m-%d %H:%M:%E6S", substring(b.EventDate, 0, length(b.EventDate)-3)
),  'America/Chicago')
, 'Europe/Rome')
)
ELSE FORMAT_DATETIME(
  "%H:%M:%E3S",
DATETIME(TIMESTAMP(PARSE_DATETIME("%Y-%m-%d %H:%M:%E3S", b.EventDate)
,  'America/Chicago')
, 'Europe/Rome')
)
END as hms_EventDate,

CASE
WHEN length(b.EventDate) > 20 THEN FORMAT_DATETIME(
  "%Y%m%d",
DATETIME(TIMESTAMP(PARSE_DATETIME("%Y-%m-%d %H:%M:%E6S", substring(b.EventDate, 0, length(b.EventDate)-3)
),  'America/Chicago')
, 'Europe/Rome')
)
ELSE FORMAT_DATETIME(
  "%Y%m%d",
DATETIME(TIMESTAMP(PARSE_DATETIME("%Y-%m-%d %H:%M:%E3S", b.EventDate),
  'America/Chicago')
, 'Europe/Rome'
)
)

END as ymd_EventDate,

CASE
WHEN length(b.EventDate) > 20 THEN FORMAT_DATETIME(
  "%Y%m%d %H:%M:%S",
DATETIME(TIMESTAMP(PARSE_DATETIME("%Y-%m-%d %H:%M:%E6S", substring(b.EventDate, 0, length(b.EventDate)-3)
),  'America/Chicago')
, 'Europe/Rome')
)
ELSE FORMAT_DATETIME(
  "%Y%m%d %H:%M:%S",
DATETIME(TIMESTAMP(PARSE_DATETIME("%Y-%m-%d %H:%M:%E3S", b.EventDate)
,  'America/Chicago')
, 'Europe/Rome'
)
)
END as ymd_hms_EventDate,

CASE
WHEN length(b.EventDate) > 20 THEN FORMAT_DATETIME(
  "%Y%m%d %H:%M:%E6S",
DATETIME(TIMESTAMP(PARSE_DATETIME("%Y-%m-%d %H:%M:%E6S", substring(b.EventDate, 0, length(b.EventDate)-3)
),  'America/Chicago')
, 'Europe/Rome')
)
ELSE FORMAT_DATETIME(
  "%Y%m%d %H:%M:%E6S",
DATETIME(TIMESTAMP(PARSE_DATETIME("%Y-%m-%d %H:%M:%E3S", b.EventDate)
,  'America/Chicago')
, 'Europe/Rome'
)
)
END as ymd_hms_ffffff_EventDate

from ETL20A.Bounce b

;


CREATE OR REPLACE TABLE `ETL20A.ClickReportPush_formatted`
as
select 
*,
FORMAT_DATETIME("%Y%m%d%H", PARSE_DATETIME("%Y%m%d %H:%M:%S", crp.SendDate))
as ymdh_sendDate,
FORMAT_DATETIME("%Y%m%d", PARSE_DATETIME("%Y%m%d %H:%M:%E6S", crp.ClickDate))
as ymd_ClickDate,
FORMAT_DATETIME("%Y%m%d %H:%M:%S", PARSE_DATETIME("%Y%m%d %H:%M:%E6S", crp.ClickDate))
as ymd_hms_ClickDate,
FORMAT_DATETIME("%Y%m%d %H:%M:%E6S", PARSE_DATETIME("%Y%m%d %H:%M:%E6S", crp.ClickDate))
as ymd_hms_ffffff_ClickDate

from ETL20A.ClickReportPush crp
where crp.SendDate like '20%' and crp.ClickDate like '%2%' and length(crp.ClickDate) <= 24 and length(crp.SendDate) <= 17

;

CREATE OR REPLACE TABLE ETL20A.MobilePushDetailExtractReport_microesito_pre
as
select * from ETL20A.MobilePushDetailExtractReport_p_failed
union all
select * from ETL20A.MobilePushDetailExtractReport_p_consegnata
union all
select * from ETL20A.MobilePushDetailExtractReport_click
;

CREATE OR REPLACE TABLE `ETL20A.MobilePushDetailExtractReport_microesito_formatted`
as
select 
*,
CASE 
WHEN length(p.DateTimeSend) < 23 THEN
FORMAT_DATETIME("%Y%m%d%H", PARSE_DATETIME("%m/%d/%Y %I:%M:%S %p", p.DateTimeSend))
ELSE FORMAT_DATETIME("%Y%m%d%H", PARSE_DATETIME("%Y%m%d %H:%M:%E6S", p.DateTimeSend))
END as ymdh_DateTimeSend,

CASE
WHEN length(p.DateTimeSend) < 23 THEN
FORMAT_DATETIME("%Y%m%d", PARSE_DATETIME("%m/%d/%Y %I:%M:%S %p", p.DateTimeSend))
ELSE FORMAT_DATETIME("%Y%m%d", PARSE_DATETIME("%Y%m%d %H:%M:%E6S", p.DateTimeSend))
END as ymd_DateTimeSend

from ETL20A.MobilePushDetailExtractReport_microesito_pre p
;

CREATE OR REPLACE TABLE `ETL20A.SmsMessageTracking`
as
select 
*,
from ETL20A.SmsMessageTracking p
where p.CreateDateTime like '2%'  and length(p.CreateDateTime) <= 24
and Delivered='1' 
union all
select 
*,
from ETL20A.SmsMessageTracking p
where p.CreateDateTime like '2%'  and length(p.CreateDateTime) <= 24
and Undelivered='1'

;

CREATE OR REPLACE TABLE `ETL20A.SmsMessageTracking_formatted`
as
select 
*,
FORMAT_DATETIME("%Y%m%d %H:%M:%S", PARSE_DATETIME("%Y-%m-%d %H:%M:%S", s.ModifiedDateTime)
) as ymd_hms_ModifiedDateTime,
FORMAT_DATETIME("%Y%m%d %H:%M:%E6S", PARSE_DATETIME("%Y-%m-%d %H:%M:%S", s.ModifiedDateTime)
) as ymd_hms_ffffff_ModifiedDateTime,
FORMAT_DATETIME("%Y%m%d %H:%M:%S", PARSE_DATETIME("%Y-%m-%d %H:%M:%S", s.ActionDateTime)
) as ymd_hms_ActionDateTime,
FORMAT_DATETIME("%Y%m%d %H:%M:%E6S", PARSE_DATETIME("%Y-%m-%d %H:%M:%S", s.ActionDateTime)
) as ymd_hms_ffffff_ActionDateTime,
FORMAT_DATETIME("%Y%m%d", PARSE_DATETIME("%Y-%m-%d %H:%M:%S", s.ActionDateTime)
) as ymd_ActionDateTime,
FORMAT_DATETIME("%Y%m%d", PARSE_DATETIME("%Y-%m-%d %H:%M:%S", s.CreateDateTime)
) as ymd_CreateDateTime,
FORMAT_DATETIME("%Y%m%d %H:%M:%S", PARSE_DATETIME("%Y-%m-%d %H:%M:%S", s.CreateDateTime)
) as ymd_hms_CreateDateTime

from ETL20A.SmsMessageTracking s
;


END;