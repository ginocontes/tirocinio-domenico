CREATE OR REPLACE TABLE marketing_final.sms AS
select
c.cod_istituto as COD_ABI, -- codice istituto bancario
c.COD_NDG_ANAGRAFICA_NSG as COD_NDG, -- codice anagrafica
c.COD_FISCALE_PARTITA_IVA as COD_FISCALE,
c.EMAIL_ADDRESS as EMAIL,
jp.name as JOURNEY_NAME,
s.logDate as DATA_INVIO,
-- u.ymd_hms_ffffff_EventDate as DATE,
m.MICROESITO as COD_MICROESITO,
"SMS" as COD_CANALE
from marketing_prep.sms s
INNER JOIN marketing_raw.cliente c on s.SubscriberKey = c.SubscriberKey
INNER JOIN marketing_prep.JourneyPrep jp  on s.journey_id = jp.id
INNER JOIN marketing_raw.microesiti m 
        on case
            when s.Undelivered = 1 THEN 'SMS_Undelivered'
            when s.Delivered = 1 THEN 'SMS_Delivered'
            ELSE 'SKIPPARE'
            END = m.ESITOMC