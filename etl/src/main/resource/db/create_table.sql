-- ========================== mysql ============================================
drop table if exists holiday;
-- auto-generated definition
create table holiday
(
    id           int auto_increment
        primary key,
    create_time  datetime not null comment '创建时间',
    update_time  datetime not null comment '更新时间',
    holiday_type int      not null comment '假期类型',
    begin_date   date     not null comment '开始日期',
    end_date     date     not null comment '结束日期'

);

create
    unique index idx_holiday
    on holiday (holiday_type, begin_date);

create table m_holiday
(holiday_type int,
cnt int
);


delete
from holiday;
delete
from holiday;
insert into holiday(holiday_type, begin_date, end_date, create_time, update_time)
values (1, '2016-12-31', '2017-01-02', current_timestamp, current_timestamp);
insert into holiday(holiday_type, begin_date, end_date, create_time, update_time)
values (2, '2017-01-27', '2017-02-02', current_timestamp, current_timestamp);
insert into holiday(holiday_type, begin_date, end_date, create_time, update_time)
values (3, '2017-04-02', '2017-04-04', current_timestamp, current_timestamp);
insert into holiday(holiday_type, begin_date, end_date, create_time, update_time)
values (4, '2017-04-29', '2017-05-01', current_timestamp, current_timestamp);
insert into holiday(holiday_type, begin_date, end_date, create_time, update_time)
values (5, '2017-05-28', '2017-05-30', current_timestamp, current_timestamp);
insert into holiday(holiday_type, begin_date, end_date, create_time, update_time)
values (7, '2017-10-01', '2017-10-08', current_timestamp, current_timestamp);
insert into holiday(holiday_type, begin_date, end_date, create_time, update_time)
values (15, '2017-11-11', '2017-11-11', current_timestamp, current_timestamp);

insert into holiday(holiday_type, begin_date, end_date, create_time, update_time)
values (1, '2017-12-30', '2018-01-01', current_timestamp, current_timestamp);
insert into holiday(holiday_type, begin_date, end_date, create_time, update_time)
values (2, '2018-02-15', '2018-02-21', current_timestamp, current_timestamp);
insert into holiday(holiday_type, begin_date, end_date, create_time, update_time)
values (3, '2018-04-05', '2018-04-07', current_timestamp, current_timestamp);
insert into holiday(holiday_type, begin_date, end_date, create_time, update_time)
values (4, '2018-04-29', '2018-05-01', current_timestamp, current_timestamp);
insert into holiday(holiday_type, begin_date, end_date, create_time, update_time)
values (5, '2018-06-16', '2018-06-18', current_timestamp, current_timestamp);
insert into holiday(holiday_type, begin_date, end_date, create_time, update_time)
values (6, '2018-09-22', '2018-09-24', current_timestamp, current_timestamp);
insert into holiday(holiday_type, begin_date, end_date, create_time, update_time)
values (7, '2018-10-01', '2018-10-07', current_timestamp, current_timestamp);
insert into holiday(holiday_type, begin_date, end_date, create_time, update_time)
values (15, '2018-11-11', '2018-11-11', current_timestamp, current_timestamp);

insert into holiday(holiday_type, begin_date, end_date, create_time, update_time)
values (1, '2018-12-30', '2019-01-01', current_timestamp, current_timestamp);
insert into holiday(holiday_type, begin_date, end_date, create_time, update_time)
values (2, '2019-02-04', '2018-02-10', current_timestamp, current_timestamp);
insert into holiday(holiday_type, begin_date, end_date, create_time, update_time)
values (3, '2019-04-05', '2019-04-07', current_timestamp, current_timestamp);
insert into holiday(holiday_type, begin_date, end_date, create_time, update_time)
values (4, '2019-05-01', '2019-05-04', current_timestamp, current_timestamp);
insert into holiday(holiday_type, begin_date, end_date, create_time, update_time)
values (5, '2019-06-07', '2019-06-09', current_timestamp, current_timestamp);
insert into holiday(holiday_type, begin_date, end_date, create_time, update_time)
values (6, '2019-09-13', '2019-09-15', current_timestamp, current_timestamp);
insert into holiday(holiday_type, begin_date, end_date, create_time, update_time)
values (7, '2019-10-01', '2019-10-07', current_timestamp, current_timestamp);
insert into holiday(holiday_type, begin_date, end_date, create_time, update_time)
values (15, '2019-11-11', '2018-11-11', current_timestamp, current_timestamp);


-- ============================= hive ======================================
create database test;
create table s_holiday
(
    id           int,
    create_time  datetime ,
    update_time  datetime ,
    holiday_type int      ,
    begin_date   date     ,
    end_date     date
);

create table m_holiday
(holiday_type int,
cnt int
);

--
