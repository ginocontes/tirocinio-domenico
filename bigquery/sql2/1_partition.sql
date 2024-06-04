
    BEGIN
    

    create or replace table marketing_source.sms_source as 
    select * ,
    PARSE_DATE("%Y-%m-%d", SPLIT(logDate, ' ')[OFFSET(0)]) as logDateDate
    from marketing_raw.sms;
    
    create or replace table marketing_source.emailclick_source as 
    select * ,
    PARSE_DATE("%m/%d/%Y", SPLIT(EventDate, ' ')[OFFSET(0)])
    as EventDateDate
    from marketing_raw.emailclick;


    create or replace table marketing_source.emailinvii_source as 
    select * ,
    PARSE_DATE("%m/%d/%Y", SPLIT(EventDate, ' ')[OFFSET(0)])
    as EventDateDate
    from marketing_raw.emailinvii;

    
    create or replace table marketing_source.notificheclick_source as 
    select * ,
    PARSE_DATE("%Y%m%d", SPLIT(ClickDate, ' ')[OFFSET(0)]) as clickDateDate
    from marketing_raw.notificheclick
    where clickDate like '%2%';
    
    

    create or replace table marketing_source.emailclick_source as 
    select * ,
    PARSE_DATE("%m/%d/%Y", SPLIT(EventDate, ' ')[OFFSET(0)])
    as EventDateDate
    from marketing_raw.emailclick;
    
    
    create or replace table marketing_source.notifiche_source as 
    select * ,
    PARSE_DATE("%m/%d/%Y", SPLIT(DateTimeSend, ' ')[OFFSET(0)])
    as DateTimeSendDate
    from marketing_raw.notifiche;

    create or replace table marketing_source.notificheclick_source as 
    select * ,
    PARSE_DATE("%Y%m%d", SPLIT(ClickDate, ' ')[OFFSET(0)]) as clickDateDate
    from marketing_raw.notificheclick
    where clickDate like '%2%';
    
    END;