
create or replace table marketing_final.esitiemail
as
SELECT
c.cod_istituto as COD_ABI, -- codice istituto bancario
c.COD_NDG_ANAGRAFICA_NSG as COD_NDG, -- codice anagrafica
c.COD_FISCALE_PARTITA_IVA as COD_FISCALE,
c.EMAIL_ADDRESS as EMAIL,
jp.name as JOURNEY_NAME,
e.data_invio as DATA_INVIO,
-- u.ymd_hms_ffffff_EventDate as DATE,
m.MICROESITO as COD_MICROESITO,
"EMAIL" as COD_CANALE
from marketing_source.emailunsub u
INNER JOIN marketing_source.emailinvii e
on u.sendid = e.sendid 
INNER JOIN marketing_raw.cliente c on  u.SubscriberKey = c.SubscriberKey
INNER JOIN marketing_raw.journey jp on jp.id = e.journey_id
INNER JOIN marketing_raw.microesiti m on m.ESITOMC = "unsubscribed"

UNION ALL

SELECT
c.cod_istituto as COD_ABI, -- codice istituto bancario
c.COD_NDG_ANAGRAFICA_NSG as COD_NDG, -- codice anagrafica
c.COD_FISCALE_PARTITA_IVA as COD_FISCALE,
c.EMAIL_ADDRESS as EMAIL_ADDRESS,
jp.name as JOURNEY_NAME,
e.data_invio as DATA_INVIO,
-- u.ymd_hms_ffffff_EventDate as DATE,
m.MICROESITO as COD_MICROESITO,
"EMAIL" as COD_CANALE
FROM marketing_source.emailclick cl
INNER JOIN marketing_source.emailinvii e
on cl.sendid = e.sendid 
INNER JOIN marketing_raw.cliente c on  cl.SubscriberKey = c.SubscriberKey
INNER JOIN marketing_raw.journey jp on jp.id = e.journey_id
INNER JOIN marketing_raw.microesiti m  on m.ESITOMC = "click"

UNION ALL

SELECT
c.cod_istituto as COD_ABI, -- codice istituto bancario
c.COD_NDG_ANAGRAFICA_NSG as COD_NDG, -- codice anagrafica
c.COD_FISCALE_PARTITA_IVA as COD_FISCALE,
c.EMAIL_ADDRESS as EMAIL_ADDRESS,
jp.name as JOURNEY_NAME,
e.data_invio as DATA_INVIO,
-- u.ymd_hms_ffffff_EventDate as DATE,
m.MICROESITO as COD_MICROESITO,
"EMAIL" as COD_CANALE
FROM marketing_source.emailinvii e
INNER JOIN marketing_raw.cliente c on  e.SubscriberKey = c.SubscriberKey
INNER JOIN marketing_raw.journey jp on jp.id = e.journey_id
INNER JOIN marketing_raw.microesiti m  on m.ESITOMC = "sent"