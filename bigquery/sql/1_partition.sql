CREATE OR REPLACE PROCEDURE `ETL20A.DataPartitioner_proc`()
OPTIONS (strict_mode=false)
BEGIN
 
create or replace table ETL20A.SendLog_temp_part as 
select * ,
PARSE_DATE("%Y-%m-%d", SPLIT(logDate, ' ')[OFFSET(0)]) as logDateDate
from ETL20A.SendLog_temp;
 
create or replace table ETL20A.SmsSendLog_temp_part as 
select * ,
PARSE_DATE("%Y-%m-%d", SPLIT(logDate, ' ')[OFFSET(0)]) as logDateDate
from ETL20A.SmsSendLog_temp;
 
create or replace table ETL20A.Unsubs_temp_part as 
select * ,
PARSE_DATE("%m/%d/%Y", SPLIT(EventDate, ' ')[OFFSET(0)])
as EventDateDate
from ETL20A.Unsubs_temp;

create or replace table ETL20A.Sent_temp_part as 
select * ,
PARSE_DATE("%m/%d/%Y", SPLIT(EventDate, ' ')[OFFSET(0)])
as EventDateDate
from ETL20A.Sent_temp;
 
create or replace table ETL20A.Complaints_temp_part as 
select * ,
PARSE_DATE("%m/%d/%Y", SPLIT(EventDate, ' ')[OFFSET(0)])
as EventDateDate
from ETL20A.Complaints_temp;
 
create or replace table ETL20A.Bounce_temp_part as 
select * ,
PARSE_DATE("%Y-%m-%d", SPLIT(EventDate, ' ')[OFFSET(0)]) as EventDateDate
from ETL20A.Bounce_temp;
 
create or replace table ETL20A.ClickReportPush_temp_part as 
select * ,
PARSE_DATE("%Y%m%d", SPLIT(ClickDate, ' ')[OFFSET(0)]) as clickDateDate
from ETL20A.ClickReportPush_temp
where clickDate like '%2%';
 
create or replace table ETL20A.SmsMessageTracking_temp_part as 
select * ,
PARSE_DATE("%Y-%m-%d", SPLIT(CreateDateTime, ' ')[OFFSET(0)]) as CreateDateTimeDate
from ETL20A.SmsMessageTracking_temp;
 
create or replace table ETL20A.Clicks_temp_part as 
select * ,
PARSE_DATE("%m/%d/%Y", SPLIT(EventDate, ' ')[OFFSET(0)])
as EventDateDate
from ETL20A.Clicks_temp;
 
create or replace table ETL20A.Opens_temp_part as 
select * ,
PARSE_DATE("%m/%d/%Y", SPLIT(EventDate, ' ')[OFFSET(0)])
as EventDateDate
from ETL20A.Opens_temp;
 
create or replace table ETL20A.MobilePushDetailExtractReport_temp_part as 
select * ,
PARSE_DATE("%m/%d/%Y", SPLIT(DateTimeSend, ' ')[OFFSET(0)])
as DateTimeSendDate
from ETL20A.MobilePushDetailExtractReport_temp;
 
 
create or replace table ETL20A.PushSendLog_temp_part as 
select * ,
PARSE_DATE("%Y-%m-%d", SPLIT(LogDate, ' ')[OFFSET(0)]) as logDateDate
from ETL20A.PushSendLog_temp;
 
END;