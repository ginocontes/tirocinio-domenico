
CREATE OR REPLACE TABLE ETL20A.CEC_CONTATTI_PUSH_NOT_CLICK AS
with t as (
select
c.surr_ndg_referente as COD_DW_NDG,
c.cod_istituto as COD_ABI,
c.COD_NDG_ANAGRAFICA_NSG as COD_NDG,
c.COD_BT as COD_BT,
c.COD_FISCALE_PARTITA_IVA,
sl.SubscriberKey as COD_TARGET_SLF,
jp.JourneyID as COD_JOURNEY_ID,
sl.ymd_hms_ffffff_logDate as TMS_CONTATTO,
CONCAT(sl.PushJobID, sl.ymd_logDate) as COD_CELL_PACKAGE_SLF,
trt.Cod_Cluster as COD_CLUSTER_SLF,
trt.OfferID as COD_OFFERTA_SLF,
case when push_type = "SUCC_CLICK" 
then "PSHOK"
else p.MICROESITO end as COD_MICROESITO,
"PSH" as COD_CANALE,
trt.rt as FLG_REAL_TIME,
trt.cod_campagna as COD_CAMPAGNA_SLF,
UPPER(jp.ActivityID) as COD_ACTIVITY_ID,
p.MessageID as COD_CREATIVITA,
"" as COD_DEM_JOB_ID,
cast(sl.PushJobID as string) as COD_PUSH_JOB_ID,
CONCAT(trt.OfferID, trt.Cod_Cluster) as COD_TREATMENT_ESTESO,
sl.PDA,
sl.callingChannel as CANALE_CHIAMANTE,
-- jp.ActivityName as JOURNEY_ACTIVITY_NAME

from ETL20A.MobilePushDetailExtractReport_microesito_formatted p
INNER JOIN ETL20A.PushSendLog_formatted sl on p.PushJobID = sl.PushJobID and p.deviceID = sl.deviceID 
and sl.ymd_logDate = p.ymd_DateTimeSend
INNER JOIN ETL20A.CustomerDetails c on sl.SubscriberKey = c.SubscriberKey
INNER JOIN ETL20A.JourneyPrep jp on p.messageName = jp.activityName
INNER JOIN ETL20A.TRTTable trt on upper(trt.ActivityID)= upper(jp.ActivityID)
where p.ymd_DateTimeSend between from_date and to_date

) select *, from t 
qualify row_number() over(partition by 
                          t.COD_TARGET_SLF,
                          t.TMS_CONTATTO,
                          t.COD_CELL_PACKAGE_SLF,
                          t.COD_JOURNEY_ID,
                          t.COD_MICROESITO,
                          t.COD_ACTIVITY_ID,
                          t.COD_PUSH_JOB_ID,
                          t.COD_CREATIVITA,
                          t.PDA,
                          t.CANALE_CHIAMANTE
                          ) = 1;


CREATE OR REPLACE TABLE ETL20A.CEC_CONTATTI_PUSH_CLICK AS
with t as (
select
c.surr_ndg_referente as COD_DW_NDG,
c.cod_istituto as COD_ABI,
c.COD_NDG_ANAGRAFICA_NSG as COD_NDG,
c.COD_BT as COD_BT,
c.COD_FISCALE_PARTITA_IVA,
p.ContactKey as COD_TARGET_SLF,
jp.JourneyID as COD_JOURNEY_ID,
crp.ymd_hms_ffffff_ClickDate as TMS_CONTATTO,
CONCAT(sl.PushJobID, sl.ymd_logDate) as COD_CELL_PACKAGE_SLF,
trt.Cod_Cluster as COD_CLUSTER_SLF,
trt.OfferID as COD_OFFERTA_SLF,
p.MICROESITO as COD_MICROESITO,
"PSH" as COD_CANALE,
trt.rt as FLG_REAL_TIME,
trt.cod_campagna as COD_CAMPAGNA_SLF,
UPPER(jp.ActivityID) as COD_ACTIVITY_ID,
p.MessageID as COD_CREATIVITA,
"" as COD_DEM_JOB_ID,
cast(sl.PushJobID as string) as COD_PUSH_JOB_ID,
CONCAT(trt.OfferID, trt.Cod_Cluster) as COD_TREATMENT_ESTESO,
sl.PDA,
sl.callingChannel as CANALE_CHIAMANTE,
-- jp.ActivityName as JOURNEY_ACTIVITY_NAME

from ETL20A.MobilePushDetailExtractReport_microesito_formatted p
INNER JOIN ETL20A.PushSendLog_formatted sl on p.PushJobID = sl.PushJobID and p.deviceID = sl.deviceID 
and sl.ymd_logDate = p.ymd_DateTimeSend
INNER JOIN ETL20A.CustomerDetails c on sl.SubscriberKey = c.SubscriberKey
INNER JOIN ETL20A.JourneyPrep jp on p.messageName = jp.activityName
INNER JOIN ETL20A.TRTTable trt on upper(trt.ActivityID)= upper(jp.ActivityID)
INNER JOIN ETL20A.ClickReportPush_formatted crp on p.deviceID = crp.deviceID and p.messageName = crp.messageName 
and p.ymdh_DateTimeSend = crp.ymdh_sendDate
where (crp.ymd_ClickDate between from_date and to_date)
and DATE_DIFF(PARSE_DATE("%Y%m%d", p.ymd_DateTimeSend), PARSE_DATE("%Y%m%d", crp.ymd_ClickDate), DAY) <= 15 
and p.push_type in ("SUCC_CLICK")
) select *, from t 
qualify row_number() over(partition by 
                          t.COD_TARGET_SLF,
                          t.TMS_CONTATTO,
                          t.COD_CELL_PACKAGE_SLF,
                          t.COD_JOURNEY_ID,
                          t.COD_MICROESITO,
                          t.COD_ACTIVITY_ID,
                          t.COD_PUSH_JOB_ID,
                          t.COD_CREATIVITA,
                          t.PDA,
                          t.CANALE_CHIAMANTE
                         ) = 1;



CREATE OR REPLACE TABLE ETL20A.CEC_CONTATTI_PUSH AS
SELECT * FROM ETL20A.CEC_CONTATTI_PUSH_NOT_CLICK
UNION ALL
SELECT * FROM ETL20A.CEC_CONTATTI_PUSH_CLICK;