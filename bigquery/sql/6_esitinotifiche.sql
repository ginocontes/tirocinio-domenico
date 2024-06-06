
CREATE OR REPLACE TABLE marketing_final.notifiche AS
select

-- jp.ActivityName as JOURNEY_ACTIVITY_NAME
c.cod_istituto as COD_ABI, -- codice istituto bancario
c.COD_NDG_ANAGRAFICA_NSG as COD_NDG, -- codice anagrafica
c.COD_FISCALE_PARTITA_IVA as COD_FISCALE,
c.EMAIL_ADDRESS as EMAIL,
jp.name as JOURNEY_NAME,
p.DateTimeSend as DATA_INVIO,
-- u.ymd_hms_ffffff_EventDate as DATE,
m.MICROESITO as COD_MICROESITO,
"NOTIFICHE" as COD_CANALE
from marketing_prep.notifiche p
INNER JOIN marketing_raw.cliente c on p.SubscriberKey = c.SubscriberKey
INNER JOIN marketing_prep.JourneyPrep jp on p.journey_id = jp.id
INNER JOIN marketing_raw.microesiti m 
        on case
            when p.not_type = "SUCC_CLICK" THEN 'notification_succ'
            when p.not_type = "SUCC_NO_CLICK" THEN 'notification_click'
            ELSE 'SKIPPARE'
            END = m.ESITOMC
