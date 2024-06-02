CREATE OR REPLACE PROCEDURE `ETL20A.CEC_CONTATTI_proc`(from_date STRING, to_date STRING)
OPTIONS (strict_mode=false)
BEGIN
CREATE OR REPLACE TABLE ETL20A.CEC_CONTATTI_DEM AS
SELECT
c.surr_ndg_referente as COD_DW_NDG,
c.cod_istituto as COD_ABI,
c.COD_NDG_ANAGRAFICA_NSG as COD_NDG,
c.COD_BT as COD_BT,
c.COD_FISCALE_PARTITA_IVA,
c.SubscriberKey as COD_TARGET_SLF,
jp.JourneyID as COD_JOURNEY_ID,
b.ymd_hms_ffffff_EventDate as TMS_CONTATTO,
CONCAT(b.JobID, s.ymd_EventDate) as COD_CELL_PACKAGE_SLF,
trt.Cod_Cluster as COD_CLUSTER_SLF,
trt.OfferID as COD_OFFERTA_SLF,
m.MICROESITO as COD_MICROESITO,
"EMA" as COD_CANALE,
trt.rt as FLG_REAL_TIME,
trt.cod_campagna as COD_CAMPAGNA_SLF,
UPPER(jp.ActivityID) as COD_ACTIVITY_ID,
job.EmailID as COD_CREATIVITA,
job.JobID as COD_DEM_JOB_ID,
"" as COD_PUSH_JOB_ID,
CONCAT(trt.OfferID, trt.Cod_Cluster) as COD_TREATMENT_ESTESO,
sl.PDA,
sl.callingChannel as CANALE_CHIAMANTE,
-- jp.ActivityName as JOURNEY_ACTIVITY_NAME

FROM ETL20A.Sent_formatted s
INNER JOIN ETL20A.Bounce_formatted b on s.sendid = b.jobid and s.listid = b.listid and s.batchid = b.batchid and s.subscriberkey = b.subscriberkey
INNER JOIN ETL20A.CustomerDetails c on b.SubscriberKey = c.SubscriberKey
INNER JOIN ETL20A.SendLog sl on b.BatchId = sl.BatchId and b.SubscriberKey = sl.SubscriberKey and sl.JobID = s.sendid
INNER JOIN ETL20A.Job job on job.JobID = sl.JobID
INNER JOIN ETL20A.JourneyPrep jp on job.TriggererSendDefinitionObjectId = jp.JourneyActivityObjectId
INNER JOIN ETL20A.TRTTable trt on upper(trt.ActivityID) = upper(jp.ActivityID)
INNER JOIN ETL20A.MicroesitiBounces m on cast(m.ESITOMC as string) = b.BounceSubcategoryID
where (b.ymd_EventDate between from_date and to_date ) and job.pickUpTime is not null 
and DATE_DIFF(PARSE_DATE("%Y%m%d", s.ymd_EventDate), PARSE_DATE("%Y%m%d", b.ymd_EventDate), DAY) <= 15 
and abs(DATETIME_DIFF(PARSE_DATETIME("%Y%m%d %H:%M:%E3S", s.ymd_hms_EventDate), PARSE_DATETIME("%Y%m%d %H:%M:%E3S", b.ymd_hms_EventDate), SECOND)) >= 1

UNION ALL

SELECT
c.surr_ndg_referente as COD_DW_NDG,
c.cod_istituto as COD_ABI,
c.COD_NDG_ANAGRAFICA_NSG as COD_NDG,
c.COD_BT as COD_BT,
c.COD_FISCALE_PARTITA_IVA,
c.SubscriberKey as COD_TARGET_SLF,
jp.JourneyID as COD_JOURNEY_ID,
cl.ymd_hms_ffffff_EventDate as TMS_CONTATTO,
CONCAT(cl.SendID, s.ymd_EventDate) as COD_CELL_PACKAGE_SLF,
trt.Cod_Cluster as COD_CLUSTER_SLF,
trt.OfferID as COD_OFFERTA_SLF,
cl.alias as COD_MICROESITO,
"EMA" as COD_CANALE,
trt.rt as FLG_REAL_TIME,
trt.cod_campagna as COD_CAMPAGNA_SLF,
UPPER(jp.ActivityID) as COD_ACTIVITY_ID,
job.EmailID as COD_CREATIVITA,
job.JobID as COD_DEM_JOB_ID,
"" as COD_PUSH_JOB_ID,
CONCAT(trt.OfferID, trt.Cod_Cluster) as COD_TREATMENT_ESTESO,
sl.PDA,
sl.callingChannel as CANALE_CHIAMANTE,
-- jp.ActivityName as JOURNEY_ACTIVITY_NAME

FROM ETL20A.Sent_formatted s
INNER JOIN ETL20A.Clicks_formatted cl on s.sendid = cl.sendid and s.listid = cl.listid and s.batchid = cl.batchid and s.subscriberkey = cl.subscriberkey
INNER JOIN ETL20A.CustomerDetails c on cl.SubscriberKey = c.SubscriberKey
INNER JOIN ETL20A.SendLog sl on cl.BatchId = sl.BatchId and cl.SubscriberKey = sl.SubscriberKey and sl.JobID = s.sendid
INNER JOIN ETL20A.Job job on job.JobID = sl.JobID
INNER JOIN ETL20A.JourneyPrep jp on job.TriggererSendDefinitionObjectId = jp.JourneyActivityObjectId
INNER JOIN ETL20A.TRTTable trt on UPPER(trt.ActivityID) = upper(jp.ActivityID)
where cl.alias is not null and job.pickUpTime is not null 
and cl.ymd_EventDate between from_date and to_date 
and DATE_DIFF(PARSE_DATE("%Y%m%d", s.ymd_EventDate), PARSE_DATE("%Y%m%d", cl.ymd_EventDate), DAY) <= 15 
and abs(DATETIME_DIFF(PARSE_DATETIME("%Y%m%d %H:%M:%E3S", s.ymd_hms_EventDate), PARSE_DATETIME("%Y%m%d %H:%M:%E3S", cl.ymd_hms_EventDate), SECOND)) >= 100

UNION ALL


SELECT
c.surr_ndg_referente as COD_DW_NDG,
c.cod_istituto as COD_ABI,
c.COD_NDG_ANAGRAFICA_NSG as COD_NDG,
c.COD_BT as COD_BT,
c.COD_FISCALE_PARTITA_IVA,
c.SubscriberKey as COD_TARGET_SLF,
jp.JourneyID as COD_JOURNEY_ID,
co.ymd_hms_ffffff_EventDate as TMS_CONTATTO,
CONCAT(co.SendID, s.ymd_EventDate) as COD_CELL_PACKAGE_SLF,
trt.Cod_Cluster as COD_CLUSTER_SLF,
trt.OfferID as COD_OFFERTA_SLF,
m.MICROESITO as COD_MICROESITO,
"EMA" as COD_CANALE,
trt.rt as FLG_REAL_TIME,
trt.cod_campagna as COD_CAMPAGNA_SLF,
UPPER(jp.ActivityID) as COD_ACTIVITY_ID,
job.EmailID as COD_CREATIVITA,
job.JobID as COD_DEM_JOB_ID,
"" as COD_PUSH_JOB_ID,
CONCAT(trt.OfferID, trt.Cod_Cluster) as COD_TREATMENT_ESTESO,
sl.PDA,
sl.callingChannel as CANALE_CHIAMANTE,
-- jp.ActivityName as JOURNEY_ACTIVITY_NAME

FROM ETL20A.Sent_formatted s
INNER JOIN ETL20A.Complaints_formatted co on s.sendid = co.sendid and s.listid = co.listid and s.batchid = co.batchid and s.subscriberkey = co.subscriberkey
INNER JOIN ETL20A.CustomerDetails c on co.SubscriberKey = c.SubscriberKey
INNER JOIN ETL20A.SendLog sl on co.BatchId = sl.BatchId and co.SubscriberKey = sl.SubscriberKey and sl.JobID = s.sendid
INNER JOIN ETL20A.Job job on job.JobID = sl.JobID
INNER JOIN ETL20A.JourneyPrep jp on job.TriggererSendDefinitionObjectId = jp.JourneyActivityObjectId
INNER JOIN ETL20A.TRTTable trt on upper(trt.ActivityID) = upper(jp.ActivityID)
INNER JOIN ETL20A.Microesiti m on m.ESITOMC = "Complaint"
where (co.ymd_EventDate between from_date and to_date ) and job.pickUpTime is not null 
and DATE_DIFF(PARSE_DATE("%Y%m%d", s.ymd_EventDate), PARSE_DATE("%Y%m%d", co.ymd_EventDate), DAY) <= 15 
and abs(DATETIME_DIFF(PARSE_DATETIME("%Y%m%d %H:%M:%E3S", s.ymd_hms_EventDate), PARSE_DATETIME("%Y%m%d %H:%M:%E3S", co.ymd_hms_EventDate), SECOND)) >= 100

UNION ALL

SELECT
c.surr_ndg_referente as COD_DW_NDG,
c.cod_istituto as COD_ABI,
c.COD_NDG_ANAGRAFICA_NSG as COD_NDG,
c.COD_BT as COD_BT,
c.COD_FISCALE_PARTITA_IVA,
c.SubscriberKey as COD_TARGET_SLF,
jp.JourneyID as COD_JOURNEY_ID,
o.ymd_hms_ffffff_EventDate as TMS_CONTATTO,
CONCAT(o.SendID, s.ymd_EventDate) as COD_CELL_PACKAGE_SLF,
trt.Cod_Cluster as COD_CLUSTER_SLF,
trt.OfferID as COD_OFFERTA_SLF,
m.MICROESITO as COD_MICROESITO,
"EMA" as COD_CANALE,
trt.rt as FLG_REAL_TIME,
trt.cod_campagna as COD_CAMPAGNA_SLF,
UPPER(jp.ActivityID) as COD_ACTIVITY_ID,
job.EmailID as COD_CREATIVITA,
job.JobID as COD_DEM_JOB_ID,
"" as COD_PUSH_JOB_ID,
CONCAT(trt.OfferID, trt.Cod_Cluster) as COD_TREATMENT_ESTESO,
sl.PDA,
sl.callingChannel as CANALE_CHIAMANTE,
-- jp.ActivityName as JOURNEY_ACTIVITY_NAME

FROM ETL20A.Sent_formatted s
INNER JOIN ETL20A.Opens_formatted o on s.sendid = o.sendid and s.listid = o.listid and s.batchid = o.batchid and s.subscriberkey = o.subscriberkey
INNER JOIN ETL20A.CustomerDetails c on o.SubscriberKey = c.SubscriberKey
INNER JOIN ETL20A.SendLog sl on o.BatchId = sl.BatchId and o.SubscriberKey = sl.SubscriberKey and sl.JobID = s.sendid
INNER JOIN ETL20A.Job job on job.JobID = sl.JobID
INNER JOIN ETL20A.JourneyPrep jp on job.TriggererSendDefinitionObjectId = jp.JourneyActivityObjectID
INNER JOIN ETL20A.TRTTable trt on upper(trt.ActivityID) = upper(jp.ActivityID)
INNER JOIN ETL20A.Microesiti m on m.ESITOMC = "Open"
where (o.ymd_EventDate between from_date and to_date)
and job.pickUpTime is not null 
and DATE_DIFF(PARSE_DATE("%Y%m%d", s.ymd_EventDate), PARSE_DATE("%Y%m%d", o.ymd_EventDate), DAY) <= 15 
and abs(DATETIME_DIFF(PARSE_DATETIME("%Y%m%d %H:%M:%E3S", s.ymd_hms_EventDate), PARSE_DATETIME("%Y%m%d %H:%M:%E3S", o.ymd_hms_EventDate), SECOND)) >= 100

UNION ALL

SELECT
c.surr_ndg_referente as COD_DW_NDG,
c.cod_istituto as COD_ABI,
c.COD_NDG_ANAGRAFICA_NSG as COD_NDG,
c.COD_BT as COD_BT,
c.COD_FISCALE_PARTITA_IVA,
c.SubscriberKey as COD_TARGET_SLF,
jp.JourneyID as COD_JOURNEY_ID,
s.ymd_hms_ffffff_EventDate as TMS_CONTATTO,
CONCAT(s.SendID, s.ymd_EventDate) as COD_CELL_PACKAGE_SLF,
trt.Cod_Cluster as COD_CLUSTER_SLF,
trt.OfferID as COD_OFFERTA_SLF,
m.MICROESITO as COD_MICROESITO,
"EMA" as COD_CANALE,
trt.rt as FLG_REAL_TIME,
trt.cod_campagna as COD_CAMPAGNA_SLF,
UPPER(jp.ActivityID) as COD_ACTIVITY_ID,
job.EmailID as COD_CREATIVITA,
job.JobID as COD_DEM_JOB_ID,
"" as COD_PUSH_JOB_ID,
CONCAT(trt.OfferID, trt.Cod_Cluster) as COD_TREATMENT_ESTESO,
sl.PDA,
sl.callingChannel as CANALE_CHIAMANTE,
-- jp.ActivityName as JOURNEY_ACTIVITY_NAME

FROM ETL20A.Sent_formatted s
INNER JOIN ETL20A.CustomerDetails c on s.SubscriberKey = c.SubscriberKey
INNER JOIN ETL20A.SendLog sl on s.BatchId = sl.BatchId and s.SubscriberKey = cast(sl.SubscriberKey as string) and sl.JobID = s.sendid
INNER JOIN ETL20A.Job job on job.JobID = sl.JobID
INNER JOIN ETL20A.JourneyPrep jp on cast(job.TriggererSendDefinitionObjectId as string) = cast(jp.JourneyActivityObjectId as string)
INNER JOIN ETL20A.TRTTable trt on upper(trt.ActivityID) = upper(jp.ActivityID)
INNER JOIN ETL20A.Microesiti m on m.ESITOMC = "Sent"
where (s.ymd_EventDate between from_date and to_date)
and job.pickUpTime is not null 

UNION ALL

SELECT
c.surr_ndg_referente as COD_DW_NDG,
c.cod_istituto as COD_ABI,
c.COD_NDG_ANAGRAFICA_NSG as COD_NDG,
c.COD_BT as COD_BT,
c.COD_FISCALE_PARTITA_IVA,
c.SubscriberKey as COD_TARGET_SLF,
jp.JourneyID as COD_JOURNEY_ID,
u.ymd_hms_ffffff_EventDate as TMS_CONTATTO,
CONCAT(u.SendID, s.ymd_EventDate) as COD_CELL_PACKAGE_SLF,
trt.Cod_Cluster as COD_CLUSTER_SLF,
trt.OfferID as COD_OFFERTA_SLF,
m.MICROESITO as COD_MICROESITO,
"EMA" as COD_CANALE,
trt.rt as FLG_REAL_TIME,
trt.cod_campagna as COD_CAMPAGNA_SLF,
UPPER(jp.ActivityID) as COD_ACTIVITY_ID,
job.EmailID as COD_CREATIVITA,
job.JobID as COD_DEM_JOB_ID,
"" as COD_PUSH_JOB_ID,
CONCAT(trt.OfferID, trt.Cod_Cluster) as COD_TREATMENT_ESTESO,
sl.PDA,
sl.callingChannel as CANALE_CHIAMANTE,
-- jp.ActivityName as JOURNEY_ACTIVITY_NAME

FROM ETL20A.Sent_formatted s
INNER JOIN ETL20A.Unsubs_formatted u on s.sendid = u.sendid and s.listid = u.listid and s.batchid = u.batchid and s.subscriberkey = u.subscriberkey
INNER JOIN ETL20A.CustomerDetails c on u.SubscriberKey = c.SubscriberKey
INNER JOIN ETL20A.SendLog sl on u.BatchId = sl.BatchId and u.SubscriberKey = cast(sl.SubscriberKey as string) and sl.JobID = s.sendid
INNER JOIN ETL20A.Job job on job.JobID = sl.JobID
INNER JOIN ETL20A.JourneyPrep jp on cast(job.TriggererSendDefinitionObjectId as string) = cast(jp.JourneyActivityObjectId as string)
INNER JOIN ETL20A.TRTTable trt on upper(trt.ActivityID) = upper(jp.ActivityID)
INNER JOIN ETL20A.Microesiti m on m.ESITOMC = "Unsubscribed"
where (u.ymd_EventDate between from_date and to_date ) and job.pickUpTime is not null 
and DATE_DIFF(PARSE_DATE("%Y%m%d", s.ymd_EventDate), PARSE_DATE("%Y%m%d", u.ymd_EventDate), DAY) <= 15 
and abs(DATETIME_DIFF(PARSE_DATETIME("%Y%m%d %H:%M:%E3S", s.ymd_hms_EventDate), PARSE_DATETIME("%Y%m%d %H:%M:%E3S", u.ymd_hms_EventDate), SECOND)) >= 100
;