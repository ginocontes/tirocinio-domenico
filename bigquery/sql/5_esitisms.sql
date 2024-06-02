CREATE OR REPLACE TABLE ETL20A.CEC_CONTATTI_SMS AS
with t as (select
c.surr_ndg_referente as COD_DW_NDG,
c.cod_istituto as COD_ABI,
c.COD_NDG_ANAGRAFICA_NSG as COD_NDG,
c.COD_BT as COD_BT,
c.COD_FISCALE_PARTITA_IVA,
s.SubscriberKey as COD_TARGET_SLF,
jp.JourneyID as COD_JOURNEY_ID,
CASE
WHEN s.Undelivered = '1' THEN s.ymd_hms_ffffff_ModifiedDateTime
WHEN s.Delivered = '1' THEN s.ymd_hms_ffffff_ActionDateTime
END as TMS_CONTATTO,
CONCAT(s.JBActivityID, s.ymd_CreateDateTime) as COD_CELL_PACKAGE_SLF,
trt.Cod_Cluster as COD_CLUSTER_SLF,
trt.OfferID as COD_OFFERTA_SLF,
m.MICROESITO as COD_MICROESITO,
"SMS" as COD_CANALE,
trt.rt as FLG_REAL_TIME,
trt.cod_campagna as COD_CAMPAGNA_SLF,
UPPER(s.JBActivityID) as COD_ACTIVITY_ID,
s.MessageID as COD_CREATIVITA,
"" as COD_DEM_JOB_ID,
"" as COD_PUSH_JOB_ID,
CONCAT(trt.OfferID, trt.Cod_Cluster) as COD_TREATMENT_ESTESO,
sl.Pda as PDA,
sl.callingChannel as CANALE_CHIAMANTE,
from ETL20A.SmsMessageTracking_formatted s
INNER JOIN ETL20A.SmsSendLog sl on s.SmsBatchID = sl.SmsBatchID and upper(sl.SmsJobID) = upper(s.SmsJobID) and sl.numeroTelefono = s.Mobile 
INNER JOIN ETL20A.CustomerDetails c on s.SubscriberKey = c.SubscriberKey
INNER JOIN ETL20A.JourneyPrep jp on upper(cast(s.JBActivityID as string)) = upper(cast(jp.ActivityID as string))
INNER JOIN ETL20A.TRTTable trt on upper(trt.ActivityID) = upper(s.JBActivityID)
INNER JOIN ETL20A.Microesiti m 
        on case
            when s.Undelivered = '1'THEN 'SMS_Undelivered'
            when s.Delivered = '1' THEN 'SMS_Delivered'
            ELSE 'SKIPPARE'
            END = m.ESITOMC
where s.ymd_CreateDateTime between from_date and to_date) select * from t qualify row_number() over(partition by 
                          t.COD_TARGET_SLF,
                          t.COD_JOURNEY_ID,
                          t.TMS_CONTATTO,
                          t.COD_CELL_PACKAGE_SLF
                         ) = 1;