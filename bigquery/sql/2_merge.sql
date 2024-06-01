CREATE OR REPLACE PROCEDURE `ETL20A.DataMerge_proc`(from_date STRING, to_date STRING)
OPTIONS(strict_mode=false)
BEGIN
MERGE `ETL20A.Bounce_all_part` a
USING `ETL20A.Bounce_temp_part` t
ON 
t.SubscriberKey = a.SubscriberKey
and t.jobid  = a.jobid
and t.listid = a.listid
and t.batchid = a.batchid
and a.EventDateDate >= parse_date("%Y%m%d", from_date)
WHEN MATCHED THEN DELETE;


MERGE `ETL20A.Bounce_all_part` a
USING `ETL20A.Bounce_temp_part` t
ON t.SubscriberKey = a.SubscriberKey
and t.jobid  = a.jobid
and t.listid = a.listid
and t.batchid = a.batchid
and a.EventDateDate >= parse_date("%Y%m%d", from_date)
WHEN NOT MATCHED THEN INSERT ROW;

CREATE OR REPLACE TABLE ETL20A.Bounce AS
select * from ETL20A.Bounce_all_part t
where t.EventDateDate between parse_date("%Y%m%d", from_date) and parse_date("%Y%m%d", to_date);

MERGE `ETL20A.ClickReportPush_all_part` a
USING `ETL20A.ClickReportPush_temp_part` t
ON t.ClickDate = a.ClickDate
and t.deviceID = a.deviceID 
and t.messageName = a.messageName 
and a.ClickDateDate >= date_sub(parse_date("%Y%m%d", from_date), INTERVAL 15 DAY)
WHEN MATCHED THEN DELETE;

MERGE `ETL20A.ClickReportPush_all_part` a
USING `ETL20A.ClickReportPush_temp_part` t
ON t.ClickDate = a.ClickDate
and t.deviceID = a.deviceID 
and t.messageName = a.messageName 
and a.ClickDateDate >= date_sub(parse_date("%Y%m%d", from_date), INTERVAL 15 DAY)
WHEN NOT MATCHED THEN INSERT ROW;


CREATE OR REPLACE TABLE ETL20A.ClickReportPush AS
select * from ETL20A.ClickReportPush_all_part t
where t.ClickDateDate >= date_sub(parse_date("%Y%m%d", to_date), INTERVAL 15 DAY) ;

MERGE `ETL20A.Clicks_all_part` a
USING `ETL20A.Clicks_temp_part` t
ON 
t.SubscriberKey = a.SubscriberKey
and t.sendid  = a.sendid
and t.listid = a.listid
and t.batchid = a.batchid
and a.EventDateDate >= parse_date("%Y%m%d", from_date)
WHEN MATCHED THEN DELETE;

MERGE `ETL20A.Clicks_all_part` a
USING `ETL20A.Clicks_temp_part` t
ON 
t.SubscriberKey = a.SubscriberKey
and t.sendid  = a.sendid
and t.listid = a.listid
and t.batchid = a.batchid
and a.EventDateDate >= parse_date("%Y%m%d", from_date)
WHEN NOT MATCHED THEN INSERT ROW;

CREATE OR REPLACE TABLE ETL20A.Clicks AS
select * from ETL20A.Clicks_all_part t
where t.EventDateDate between parse_date("%Y%m%d", from_date) and parse_date("%Y%m%d", to_date);

MERGE `ETL20A.Complaints_all_part` a
USING `ETL20A.Complaints_temp_part` t
ON 
t.SubscriberKey = a.SubscriberKey
and t.sendid  = a.sendid
and t.listid = a.listid
and t.batchid = a.batchid
and a.EventDateDate >= parse_date("%Y%m%d", from_date)
WHEN MATCHED THEN DELETE;

MERGE `ETL20A.Complaints_all_part` a
USING `ETL20A.Complaints_temp_part` t
ON 
t.SubscriberKey = a.SubscriberKey
and t.sendid  = a.sendid
and t.listid = a.listid
and t.batchid = a.batchid
and a.EventDateDate >= parse_date("%Y%m%d", from_date)
WHEN NOT MATCHED THEN INSERT ROW;

CREATE OR REPLACE TABLE ETL20A.Complaints AS
select * from ETL20A.Complaints_all_part t
where t.EventDateDate between parse_date("%Y%m%d", from_date) and parse_date("%Y%m%d", to_date);

MERGE `ETL20A.MobilePushDetailExtractReport_all_part` a
USING `ETL20A.MobilePushDetailExtractReport_temp_part` t
ON 
t.PushJobID = a.PushJobID
and t.deviceID = a.deviceID
and t.DateTimeSend = a.DateTimeSend
and a.DateTimeSendDate >= parse_date("%Y%m%d", from_date)
WHEN MATCHED THEN DELETE;

MERGE `ETL20A.MobilePushDetailExtractReport_all_part` a
USING `ETL20A.MobilePushDetailExtractReport_temp_part` t
ON 
t.PushJobID = a.PushJobID
and t.deviceID = a.deviceID
and t.DateTimeSend = a.DateTimeSend
and a.DateTimeSendDate >= parse_date("%Y%m%d", from_date)
WHEN NOT MATCHED THEN INSERT ROW;

CREATE OR REPLACE TABLE ETL20A.MobilePushDetailExtractReport AS
select * from ETL20A.MobilePushDetailExtractReport_all_part t
where t.DateTimeSendDate between date_sub(parse_date("%Y%m%d", from_date), INTERVAL 15 DAY) and parse_date("%Y%m%d", to_date);

MERGE `ETL20A.Opens_all_part` a
USING `ETL20A.Opens_temp_part` t
ON 
t.SubscriberKey = a.SubscriberKey
and t.sendid  = a.sendid
and t.listid = a.listid
and t.batchid = a.batchid
and a.EventDateDate >= parse_date("%Y%m%d", from_date)
WHEN MATCHED THEN DELETE;

MERGE `ETL20A.Opens_all_part` a
USING `ETL20A.Opens_temp_part` t
ON 
t.SubscriberKey = a.SubscriberKey
and t.sendid  = a.sendid
and t.listid = a.listid
and t.batchid = a.batchid
and a.EventDateDate >= parse_date("%Y%m%d", from_date)
WHEN NOT MATCHED THEN INSERT ROW;

CREATE OR REPLACE TABLE ETL20A.Opens AS
select * from ETL20A.Opens_all_part t
where t.EventDateDate between parse_date("%Y%m%d", from_date) and parse_date("%Y%m%d", to_date);

MERGE `ETL20A.PushSendLog_all_part` a
USING `ETL20A.PushSendLog_temp_part` t
ON 
t.PushJobID = a.PushJobID
and t.deviceID = a.deviceID
and t.PushBatchID = a.PushBatchID
and t.LogDate = a.LogDate
and a.logDateDate >= date_sub(parse_date("%Y%m%d", from_date), INTERVAL 15 DAY)
WHEN MATCHED THEN DELETE;


MERGE `ETL20A.PushSendLog_all_part` a
USING `ETL20A.PushSendLog_temp_part` t
ON 
t.PushJobID = a.PushJobID
and t.deviceID = a.deviceID
and t.PushBatchID = a.PushBatchID
and t.LogDate = a.LogDate
and a.logDateDate >= date_sub(parse_date("%Y%m%d", from_date), INTERVAL 15 DAY)
WHEN NOT MATCHED THEN INSERT ROW;

CREATE OR REPLACE TABLE ETL20A.PushSendLog AS
select * from ETL20A.PushSendLog_all_part t
where t.logDateDate between parse_date("%Y%m%d", from_date) and parse_date("%Y%m%d", to_date);

MERGE `ETL20A.SendLog_all_part` a
USING `ETL20A.SendLog_temp_part` t
ON t.BatchId = a.BatchId
and t.SubscriberKey = a.SubscriberKey
and t.JobID = a.JobID
and t.LogDate = a.LogDate
and a.logDateDate >= date_sub(parse_date("%Y%m%d", from_date), INTERVAL 15 DAY)
WHEN MATCHED THEN DELETE;

MERGE `ETL20A.SendLog_all_part` a
USING `ETL20A.SendLog_temp_part` t
ON t.BatchId = a.BatchId
and t.SubscriberKey = a.SubscriberKey
and t.JobID = a.JobID
and t.LogDate = a.LogDate
and a.logDateDate >= date_sub(parse_date("%Y%m%d", from_date), INTERVAL 15 DAY)
WHEN NOT MATCHED THEN INSERT ROW;

CREATE OR REPLACE TABLE ETL20A.SendLog AS
select * from ETL20A.SendLog_all_part t
where t.logDateDate between date_sub(parse_date("%Y%m%d", from_date), INTERVAL 15 DAY) and parse_date("%Y%m%d", to_date);

MERGE `ETL20A.Sent_all_part` a
USING `ETL20A.Sent_temp_part` t
ON 
t.SubscriberKey = a.SubscriberKey
and t.sendid  = a.sendid
and t.listid = a.listid
and t.batchid = a.batchid
and a.EventDateDate >= date_sub(parse_date("%Y%m%d", from_date), INTERVAL 15 DAY)
WHEN MATCHED THEN DELETE;

MERGE `ETL20A.Sent_all_part` a
USING `ETL20A.Sent_temp_part` t
ON 
t.SubscriberKey = a.SubscriberKey
and t.sendid  = a.sendid
and t.listid = a.listid
and t.batchid = a.batchid
and a.EventDateDate >= date_sub(parse_date("%Y%m%d", from_date), INTERVAL 15 DAY)
WHEN NOT MATCHED THEN INSERT ROW;

CREATE OR REPLACE TABLE ETL20A.Sent AS
select * from ETL20A.Sent_all_part t
where t.EventDateDate between date_sub(parse_date("%Y%m%d", from_date), INTERVAL 15 DAY) and parse_date("%Y%m%d", to_date);

MERGE `ETL20A.SmsSendLog_all_part` a
USING `ETL20A.SmsSendLog_temp_part` t
ON 
t.SmsJobID = a.SmsJobID
and t.SmsBatchID = a.SmsBatchID
and t.SubscriberKey = a.SubscriberKey
and t.LogDate = a.LogDate
and a.logDateDate >= date_sub(parse_date("%Y%m%d", from_date), INTERVAL 15 DAY)
WHEN MATCHED THEN DELETE;

MERGE `ETL20A.SmsSendLog_all_part` a
USING `ETL20A.SmsSendLog_temp_part` t
ON 
t.SmsJobID = a.SmsJobID
and t.SmsBatchID = a.SmsBatchID
and t.SubscriberKey = a.SubscriberKey
and t.LogDate = a.LogDate
and a.logDateDate >= date_sub(parse_date("%Y%m%d", from_date), INTERVAL 15 DAY)
WHEN NOT MATCHED THEN INSERT ROW;

CREATE OR REPLACE TABLE ETL20A.SmsSendLog AS
select * from ETL20A.SmsSendLog_all_part t
where t.logDateDate between date_sub(parse_date("%Y%m%d", from_date), INTERVAL 15 DAY) and parse_date("%Y%m%d", to_date);

MERGE `ETL20A.SmsMessageTracking_all_part` a
USING `ETL20A.SmsMessageTracking_temp_part` t
ON 
t.SmsJobID = a.SmsJobID
and t.SmsBatchID = a.SmsBatchID
and t.SubscriberKey = a.SubscriberKey
and t.ModifiedDateTime = a.ModifiedDateTime
and a.CreateDateTimeDate >= parse_date("%Y%m%d", from_date)
WHEN MATCHED THEN DELETE;

MERGE `ETL20A.SmsMessageTracking_all_part` a
USING `ETL20A.SmsMessageTracking_temp_part` t
ON 
t.SmsJobID = a.SmsJobID
and t.SmsBatchID = a.SmsBatchID
and t.SubscriberKey = a.SubscriberKey
and t.ModifiedDateTime = a.ModifiedDateTime
and a.CreateDateTimeDate >= parse_date("%Y%m%d", from_date)
WHEN NOT MATCHED THEN INSERT ROW;

CREATE OR REPLACE TABLE ETL20A.SmsMessageTracking AS
select * from ETL20A.SmsMessageTracking_all_part t
where t.CreateDateTimeDate between parse_date("%Y%m%d", from_date) and parse_date("%Y%m%d", to_date);


MERGE `ETL20A.Unsubs_all_part` a
USING `ETL20A.Unsubs_temp_part` t
ON 
t.SubscriberKey = a.SubscriberKey
and t.sendid  = a.sendid
and t.listid = a.listid
and t.batchid = a.batchid
and a.EventDateDate >= parse_date("%Y%m%d", from_date)
WHEN MATCHED THEN DELETE;

MERGE `ETL20A.Unsubs_all_part` a
USING `ETL20A.Unsubs_temp_part` t
ON 
t.SubscriberKey = a.SubscriberKey
and t.sendid  = a.sendid
and t.listid = a.listid
and t.batchid = a.batchid
and a.EventDateDate >= parse_date("%Y%m%d", from_date)
WHEN NOT MATCHED THEN INSERT ROW;

CREATE OR REPLACE TABLE ETL20A.Unsubs AS
select * from ETL20A.Unsubs_all_part t
where t.EventDateDate between parse_date("%Y%m%d", from_date) and parse_date("%Y%m%d", to_date);

END;