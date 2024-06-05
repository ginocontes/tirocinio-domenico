
MERGE marketing_source.sms_all a
USING marketing_source.sms_source t
ON 
a.id = t.id
WHEN NOT MATCHED THEN INSERT ROW;

CREATE OR REPLACE TABLE marketing_source.sms AS
select * from marketing_source.sms_all t
where t.logDateDate > current_date - 3 ;

MERGE marketing_source.emailinvii_all AS a
USING marketing_source.emailinvii_source t
ON 
a.sendid = t.sendid
WHEN NOT MATCHED THEN INSERT ROW;

CREATE OR REPLACE TABLE marketing_source.emailinvii AS
select * from marketing_source.emailinvii_all t
where t.EventDateDate > current_date - 3 ;


MERGE marketing_source.emailclick_all a
USING marketing_source.emailclick_source t
ON 
a.sendid = t.sendid
WHEN NOT MATCHED THEN INSERT ROW;

CREATE OR REPLACE TABLE marketing_source.emailclick AS
select * from marketing_source.emailclick_all t
where t.EventDateDate > current_date - 3 ;


MERGE marketing_source.emailunsub_all a
USING marketing_source.emailunsub_source t
ON 
a.sendid = t.sendid
WHEN NOT MATCHED THEN INSERT ROW;

CREATE OR REPLACE TABLE marketing_source.emailunsub AS
select * from marketing_source.emailunsub_all t
where t.EventDateDate > current_date - 3 ;


MERGE marketing_source.notifiche_all a
USING marketing_source.notifiche_source t
ON 
a.id = t.id
WHEN NOT MATCHED THEN INSERT ROW;

CREATE OR REPLACE TABLE marketing_source.notifiche AS
select * from marketing_source.notifiche_all t
where t.DateTimeSendDate > current_date - 3 ;

MERGE marketing_source.notificheclick_all a
USING marketing_source.notificheclick_source t
ON 
a.id = t.id
WHEN NOT MATCHED THEN INSERT ROW;

CREATE OR REPLACE TABLE marketing_source.notificheclick AS
select * from marketing_source.notificheclick_all t
where t.clickDateDate > current_date - 3 ;