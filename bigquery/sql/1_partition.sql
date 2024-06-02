CREATE OR REPLACE PROCEDURE `ETL20A.DataPartitioner_proc`()
OPTIONS (strict_mode=false)
BEGIN
 
create or replace table ETL20A.SendLog_stage_part as 
select * ,
PARSE_DATE("%Y-%m-%d", SPLIT(logDate, ' ')[OFFSET(0)]) as logDateDate
from ETL20A.SendLog_stage;
 
create or replace table ETL20A.SmsSendLog_stage_part as 
select * ,
PARSE_DATE("%Y-%m-%d", SPLIT(logDate, ' ')[OFFSET(0)]) as logDateDate
from ETL20A.SmsSendLog_stage;
 
create or replace table ETL20A.Unsubs_stage_part as 
select * ,
PARSE_DATE("%m/%d/%Y", SPLIT(EventDate, ' ')[OFFSET(0)])
as EventDateDate
from ETL20A.Unsubs_stage;

create or replace table ETL20A.Sent_stage_part as 
select * ,
PARSE_DATE("%m/%d/%Y", SPLIT(EventDate, ' ')[OFFSET(0)])
as EventDateDate
from ETL20A.Sent_stage;
 
create or replace table ETL20A.Complaints_stage_part as 
select * ,
PARSE_DATE("%m/%d/%Y", SPLIT(EventDate, ' ')[OFFSET(0)])
as EventDateDate
from ETL20A.Complaints_stage;
 
create or replace table ETL20A.Bounce_stage_part as 
select * ,
PARSE_DATE("%Y-%m-%d", SPLIT(EventDate, ' ')[OFFSET(0)]) as EventDateDate
from ETL20A.Bounce_stage;
 
create or replace table ETL20A.ClickReportPush_stage_part as 
select * ,
PARSE_DATE("%Y%m%d", SPLIT(ClickDate, ' ')[OFFSET(0)]) as clickDateDate
from ETL20A.ClickReportPush_stage
where clickDate like '%2%';
 
create or replace table ETL20A.SmsMessageTracking_stage_part as 
select * ,
PARSE_DATE("%Y-%m-%d", SPLIT(CreateDateTime, ' ')[OFFSET(0)]) as CreateDateTimeDate
from ETL20A.SmsMessageTracking_stage;
 
create or replace table ETL20A.Clicks_stage_part as 
select * ,
PARSE_DATE("%m/%d/%Y", SPLIT(EventDate, ' ')[OFFSET(0)])
as EventDateDate
from ETL20A.Clicks_stage;
 
create or replace table ETL20A.Opens_stage_part as 
select * ,
PARSE_DATE("%m/%d/%Y", SPLIT(EventDate, ' ')[OFFSET(0)])
as EventDateDate
from ETL20A.Opens_stage;
 
create or replace table ETL20A.MobilePushDetailExtractReport_stage_part as 
select * ,
PARSE_DATE("%m/%d/%Y", SPLIT(DateTimeSend, ' ')[OFFSET(0)])
as DateTimeSendDate
from ETL20A.MobilePushDetailExtractReport_stage;
 
 
create or replace table ETL20A.PushSendLog_stage_part as 
select * ,
PARSE_DATE("%Y-%m-%d", SPLIT(LogDate, ' ')[OFFSET(0)]) as logDateDate
from ETL20A.PushSendLog_stage;
 
END;