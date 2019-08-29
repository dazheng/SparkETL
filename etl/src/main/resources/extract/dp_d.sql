insert overwrite table stg.s_holiday
select id,create_time,update_time,holiday_type,begin_date,end_date from holiday;
