drop table individual_user;
go
create table individual_user
(
    id               bigint primary key not null ,
    username         nvarchar(45) not null,
    password         nvarchar(45) not null ,
    mobile_phone     int  not null default 0 ,
    email            varchar(45) not null default '' ,
    status           tinyint  not null default 1 ,
    create_time      datetime    not null default current_timestamp,
    create_user      bigint  not null,
    last_modify_time datetime    not null default current_timestamp,
    last_modify_user bigint  not null,
    is_deleted       bigint  not null default 0
);
go

drop  table shop
go
create table shop
(
    id               bigint primary key   not null,
    user_id          bigint  not null,
    shop_name        nvarchar(45) not null,
    create_time      datetime    not null default current_timestamp,
    create_user      bigint  not null,
    last_modify_time datetime    not null default current_timestamp,
    last_modify_user bigint  not null,
    is_deleted       bigint  not null default 0
);
go

drop table item;
go
create table item
(
    id                bigint primary key  not null,
    shop_id           bigint  not null ,
    item_type         tinyint  not null default 1,
    item_name         nvarchar(45) not null,
    category_one_id   bigint  null,
    category_two_id   bigint  null,
    category_three_id bigint  null,
    create_time       datetime    not null default current_timestamp,
    create_user       bigint  not null,
    last_modify_time  datetime    not null default current_timestamp,
    last_modify_user  bigint  not null,
    is_deleted        bigint  not null default 0
);
go

drop table warehouse;
create table warehouse
(
    id               bigint  primary key not null,
    user_id          bigint  not null,
    shop_id          bigint  not null,
    warehouse_type   tinyint  not null default 1,
    warehouse_name   nvarchar(45) not null,
    create_time      datetime    not null default current_timestamp,
    create_user      bigint  not null,
    last_modify_time datetime    not null default current_timestamp,
    last_modify_user bigint  not null,
    is_deleted       bigint  not null default 0
);
go

drop table m_item_type;
go
create table m_item_type
(
    time_type    tinyint     not null,
    time_id      varchar(10) not null,
    item_type    smallint     not null,
    user_no      int         not null,
    item_no      int         not null,
    shop_no      int         not null,
    warehouse_no int         not null,
    primary key(time_type, time_id, item_type)
) ;
go
