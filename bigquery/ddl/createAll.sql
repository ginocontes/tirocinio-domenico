create or replace table marketing_source.sms_all
as
select * from marketing_source.sms_source;

create or replace table marketing_source.emailclick_all
as
select * from `etl-tesi-domenico.marketing_source.emailclick_source`;

create or replace table marketing_source.emailinvii_all
as
select * from `etl-tesi-domenico.marketing_source.emailinvii_source`;

create or replace table marketing_source.notifiche_all
as
select * from `etl-tesi-domenico.marketing_source.notifiche_source`;

create or replace table marketing_source.notificheclick_all
as
select * from `etl-tesi-domenico.marketing_source.notificheclick_source`;

create or replace table marketing_source.emailunsub_all
as
select * from `etl-tesi-domenico.marketing_source.emailunsub_source`;


create or replace table marketing_prep.campaign_full as
select
j1.id as campaignId,
j1.name as campaignName,
j1.activity_id as ActivityID,
j1.Activity_Name as ActivityName,
current_timestamp as INSERT_TIMESTAMP
FROM marketing_prep.JourneyPrep j1

