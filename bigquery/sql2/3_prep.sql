CREATE OR REPLACE TABLE marketing_prep.JourneyPrep
as
SELECT
j.*,
ja.id as activity_id,
ja.Activity_Name
FROM marketing_raw.journey j
INNER JOIN marketing_raw.journeyActivity ja on j.id = ja.JourneyID
where j.JourneyStatus='Running' or j.JourneyStatus='Finishing' or j.JourneyStatus='Stopped' or j.JourneyStatus='Paused'
;

CREATE OR REPLACE TABLE marketing_prep.notifiche
as
-- select 
-- *,
-- FORMAT_DATETIME(
--   "%Y%m%d",
-- DATETIME(TIMESTAMP(PARSE_DATETIME("%d/%m/%Y %H:%M:%E6S", substring(sl.DateTimeSend, 0, length(sl.DateTimeSend)-3)),
--  'America/Chicago'), 'Europe/Rome'
--  )
-- )
--  as ymd_logDate,
--  FORMAT_DATETIME(
--   "%Y%m%d%H",
-- DATETIME(TIMESTAMP(PARSE_DATETIME("%d/%m/%Y %H:%M:%E6S", substring(sl.DateTimeSend, 0, length(sl.DateTimeSend)-3)),
--  'America/Chicago'), 'Europe/Rome')
-- )
-- as ymdh_logDate,
-- FORMAT_DATETIME(
--   "%Y%m%d %H:%M:%S",
-- DATETIME(TIMESTAMP(PARSE_DATETIME("%d/%m/%Y %H:%M:%E6S", substring(sl.DateTimeSend, 0, length(sl.DateTimeSend)-3)),
--  'America/Chicago'), 'Europe/Rome')
-- )
-- as ymd_hms_DateTimeSend,

-- FORMAT_DATETIME(
--   "%Y%m%d %H:%M:%E6S",
-- DATETIME(TIMESTAMP(PARSE_DATETIME("%d/%m/%Y %H:%M:%E6S", substring(sl.DateTimeSend, 0, length(sl.DateTimeSend)-3)),
--  'America/Chicago'), 'Europe/Rome')
-- )
-- as ymd_hms_ffffff_DateTimeSend

-- from marketing_source.notifiche sl
-- where length(cast(sl.DateTimeSend as string)) > 19

-- union all

select
*,
FORMAT_DATETIME(
  "%Y%m%d",
DATETIME(TIMESTAMP(PARSE_DATETIME("%d/%m/%Y %H:%M:%S", sl.DateTimeSend),
 'America/Chicago'), 'Europe/Rome')
)
as ymd_DateTimeSend,
FORMAT_DATETIME(
  "%Y%m%d%H",
DATETIME(TIMESTAMP(PARSE_DATETIME("%d/%m/%Y %H:%M:%S", sl.DateTimeSend),
 'America/Chicago'), 'Europe/Rome')
)
as ymdh_DateTimeSend,
FORMAT_DATETIME(
  "%Y%m%d %H:%M:%S",
DATETIME(TIMESTAMP(PARSE_DATETIME("%d/%m/%Y %H:%M:%S", sl.DateTimeSend),
'America/Chicago'), 'Europe/Rome')
)
as ymd_hms_DateTimeSend,
FORMAT_DATETIME(
  "%Y%m%d %H:%M:%E6S",
DATETIME(TIMESTAMP(PARSE_DATETIME("%d/%m/%Y %H:%M:%S", sl.DateTimeSend),
 'America/Chicago'), 'Europe/Rome')
)
as ymd_hms_ffffff_DateTimeSend

from marketing_source.notifiche sl
where length(cast(sl.DateTimeSend as string)) <= 19;



CREATE OR REPLACE TABLE marketing_prep.notifiche
as
select 
p.*,
"SUCC_NO_CLICK" as not_type,

from marketing_prep.notifiche p
LEFT OUTER JOIN marketing_source.notificheclick nc on cast(p.deviceID AS STRING) = CAST(nc.deviceID AS STRING) and CAST(p.id AS STRING) = CAST(nc.notificaID  AS STRING)
where upper(p.status) = upper("success")
and nc.deviceID is null 
; 




CREATE OR REPLACE TABLE marketing_prep.notificheclick
as
select 
distinct
p.*except(not_type),
"SUCC_CLICK" as not_type,

from marketing_prep.notifiche p
INNER JOIN marketing_source.notificheclick nc on CAST(p.deviceID AS STRING) = CAST(nc.deviceID AS STRING) and CAST(p.id AS STRING) = cast(nc.notificaID AS STRING)
;





-- Qui sdoppiamo le delivered e undelivered
CREATE OR REPLACE TABLE marketing_prep.sms
as
select 
*,
from marketing_source.sms p
where delivered=1
union all
select 
*,
from marketing_source.sms p
where undelivered=1

;