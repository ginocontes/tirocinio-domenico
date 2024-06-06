DROP TABLE `etl-tesi-domenico.marketing_source.emailinvii_all`; 

CREATE OR REPLACE TABLE `etl-tesi-domenico.marketing_source.emailinvii_all`
(
  journey_id INT64,
  sendid INT64,
  data_invio STRING,
  EventDate STRING,
  SubscriberKey INT64,
  EventDateDate DATE
) PARTITION BY EventDateDate;

DROP TABLE `etl-tesi-domenico.marketing_source.emailunsub_all`; 

CREATE OR REPLACE TABLE `etl-tesi-domenico.marketing_source.emailunsub_all`
(
  sendid INT64,
  EventDate STRING,
  SubscriberKey INT64,
  EventDateDate DATE
) PARTITION BY EventDateDate;

DROP TABLE `etl-tesi-domenico.marketing_source.emailclick_all`; 

CREATE OR REPLACE TABLE `etl-tesi-domenico.marketing_source.emailclick_all`
(
  sendid INT64,
  EventDate STRING,
  SubscriberKey INT64,
  EventDateDate DATE
) PARTITION BY EventDateDate;

DROP TABLE `etl-tesi-domenico.marketing_source.notifiche_all`; 

CREATE OR REPLACE TABLE `etl-tesi-domenico.marketing_source.notifiche_all`
(
  id INT64,
  DateTimeSend STRING,
  deviceId STRING,
  testo STRING,
  status STRING,
  SubscriberKey INT64,
  journey_id INT64,
  DateTimeSendDate DATE
) PARTITION BY DateTimeSendDate;

DROP TABLE `etl-tesi-domenico.marketing_source.notificheclick_all`; 

CREATE OR REPLACE TABLE `etl-tesi-domenico.marketing_source.notificheclick_all`
(
  id INT64,
  ClickDate STRING,
  deviceId STRING,
  notificaId STRING,
  clickDateDate DATE
) PARTITION BY clickDateDate;

DROP TABLE `etl-tesi-domenico.marketing_source.sms_all`; 

CREATE OR REPLACE TABLE `etl-tesi-domenico.marketing_source.sms_all`
(
  id INT64,
  logDate STRING,
  delivered INT64,
  undelivered INT64,
  SubscriberKey INT64,
  journey_id INT64,
  logDateDate DATE
) PARTITION BY logDateDate;
