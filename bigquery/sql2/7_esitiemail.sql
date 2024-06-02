
create or replace table marketing_final.esitiemail
as
SELECT
c.cod_istituto as COD_ABI, -- codice istituto bancario
c.COD_NDG_ANAGRAFICA_NSG as COD_NDG, -- codice anagrafica
c.COD_FISCALE_PARTITA_IVA as COD_FISCALE,
c.EMAIL_ADDRESS as EMAIL,
jp.name as COD_JOURNEY_ID,
e.data_invio as DATA_INVIO,
u.ymd_hms_ffffff_EventDate as DATE,
m.MICROESITO as COD_MICROESITO,
"EMAIL" as COD_CANALE
FROM marketing_prep.emailunsub_prep u
INNER JOIN marketing_prep.emailinvii_prep e,
on u.sendid = e.sendid 
INNER JOIN marketing_raw.cliente c on  u.SubscriberKey = c.SubscriberKey
INNER JOIN marketing_prep.journey_prep jp on jp.id = e.journey_id
INNER JOIN marketing_raw.microesiti m on m.ESITOMC = "unsubscribed"

UNION ALL

SELECT
c.cod_istituto as COD_ABI, -- codice istituto bancario
c.COD_NDG_ANAGRAFICA_NSG as COD_NDG, -- codice anagrafica
c.COD_FISCALE_PARTITA_IVA as COD_FISCALE,
c.EMAIL as EMAIL_DOMAIN,
jp.JourneyID as COD_JOURNEY_ID,
e.data_invio as DATA_INVIO,
u.ymd_hms_ffffff_EventDate as DATE,
m.MICROESITO as COD_MICROESITO,
"EMAIL" as COD_CANALE
FROM marketing_prep.emailopen_prep u
INNER JOIN marketing_prep.emailinvii_prep e
on u.sendid = e.sendid 
INNER JOIN marketing_raw.cliente c on  u.SubscriberKey = c.SubscriberKey
INNER JOIN marketing_prep.journey_prep jp on jp.id = e.journey_id
INNER JOIN marketing_raw.microesiti m on m.ESITOMC = "open"

UNION ALL

SELECT
c.cod_istituto as COD_ABI, -- codice istituto bancario
c.COD_NDG_ANAGRAFICA_NSG as COD_NDG, -- codice anagrafica
c.COD_FISCALE_PARTITA_IVA as COD_FISCALE,
c.EMAIL as EMAIL_DOMAIN,
jp.JourneyID as COD_JOURNEY_ID,
e.data_invio as DATA_INVIO,
u.ymd_hms_ffffff_EventDate as DATE,
m.MICROESITO as COD_MICROESITO,
"EMAIL" as COD_CANALE
FROM marketing_prep.emailclick_prep u
INNER JOIN marketing_prep.emailinvii_prep e
on u.sendid = e.sendid 
INNER JOIN marketing_raw.cliente c on  u.SubscriberKey = c.SubscriberKey
INNER JOIN marketing_prep.journey_prep jp on jp.id = e.journey_id
INNER JOIN marketing_raw.microesiti m on m.ESITOMC = "click"







