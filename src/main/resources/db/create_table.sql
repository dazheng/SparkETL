-- mysql --
create table if not exists user
(
    id               bigint unsigned not null,
    username         varchar(45) not null comment '用户名',
    password         varchar(45) not null comment '密码',
    mobile_phone     int unsigned not null default 0 comment '手机',
    email            varchar(45) not null default '' comment '邮箱',
    status           tinyint unsigned not null default 1 comment '状态. \n1 : 正常',
    create_time      datetime    not null default current_timestamp,
    create_user      bigint unsigned not null,
    last_modify_time datetime    not null default current_timestamp on update current_timestamp,
    last_modify_user bigint unsigned not null,
    is_deleted       bigint unsigned not null default 0,
    primary key (id),
    unique index idx_mobile_phone (mobile_phone asc),
    unique index idx_email (email asc),
    unique index idx_username (username asc)
)
    comment = '个人用户－基本信息';


create table if not exists shop
(
    id               bigint unsigned not null,
    user_id          bigint unsigned not null comment '所属用户id.',
    shop_name        varchar(45) not null comment '店铺名称',
    create_time      datetime    not null default current_timestamp,
    create_user      bigint unsigned not null,
    last_modify_time datetime    not null default current_timestamp on update current_timestamp,
    last_modify_user bigint unsigned not null,
    is_deleted       bigint unsigned not null default 0,
    primary key (id),
    index fk_user_id_idx (user_id asc)
)
    comment = '店铺信息';


create table if not exists item
(
    id                bigint unsigned not null,
    shop_id           bigint unsigned not null comment '所属店铺id . fk shop.id',
    item_type         tinyint unsigned not null default 1 comment '商品类型 .\n0 . 简单类型，比如：书\n1 . 多规格类型：比如：衣服\n参考 : https://learnwoo.com/woocommerce-different-product-types/',
    item_name         varchar(45) not null comment '商品名称',
    category_one_id   bigint unsigned null comment '一级分类 . fk item_category.id',
    category_two_id   bigint unsigned null comment '二级分类 . fk item_category.id',
    category_three_id bigint unsigned null comment '三级分类 . fk item_category.id',
    create_time       datetime    not null default current_timestamp,
    create_user       bigint unsigned not null,
    last_modify_time  datetime    not null default current_timestamp on update current_timestamp,
    last_modify_user  bigint unsigned not null,
    is_deleted        bigint unsigned not null default 0,
    primary key (id),
    index fk_shop_id_idx (shop_id asc),
    index fk_category_one_id_idx (category_one_id asc),
    index fk_category_two_id_idx (category_two_id asc),
    index fk_category_three_id_idx (category_three_id asc)
)
    comment = '商品';


create table if not exists warehouse
(
    id               bigint unsigned not null,
    user_id          bigint unsigned not null comment '所属用户id',
    shop_id          bigint unsigned not null comment '所属店铺id',
    warehouse_type   tinyint unsigned not null default 1 comment '综合仓类型 . \n\n1. virtual . 没有实际仓库\n2. solid . 实体仓库 . 没错 ,　就是这个单词 , 从美剧中学的 .  ',
    warehouse_name   varchar(45) not null comment '仓库名称',
    create_time      datetime    not null default current_timestamp,
    create_user      bigint unsigned not null,
    last_modify_time datetime    not null default current_timestamp on update current_timestamp,
    last_modify_user bigint unsigned not null,
    is_deleted       bigint unsigned not null default 0,
    primary key (id),
    index fk_individual_user_id_idx (user_id asc),
    index fk_shop_id_idx (shop_id asc)
)
    comment = '仓库. 代表存放商品的仓库.';

create table if not exists m_item_type
(
    time_type    tinyint     not null comment '时间类型',
    time_id      varchar(10) not null comment '时间id',
    item_type    tinyint     not null comment '商品类型',
    user_no      int         not null comment '商家数',
    item_no      int         not null comment '商品数',
    shop_no      int         not null comment '店铺数',
    warehouse_no int         not null comment '仓库数'
) comment '商品类型集市表';

-- hive
create database stg;
create database inte;
create database dm;

use stg;
drop table if exists s_user;
create table s_user
(
    id               bigint,
    user_name        varchar(45) comment '用户名',
    password         varchar(45) comment '密码',
    mobile_phone     int comment '手机',
    email            varchar(45) comment '邮箱',
    status           tinyint comment '状态. \n1 : 正常',
    create_time      timestamp,
    create_user      bigint,
    last_modify_time timestamp,
    last_modify_user bigint,
    is_deleted       bigint
)
    comment '个人用户－基本信息'
    partitioned by (time_type tinyint, time_id varchar(10))
    stored as textfile;


drop table if exists s_shop;
create table s_shop
(
    id               bigint,
    user_id          bigint comment '所属用户id.',
    shop_name        varchar(45) comment '店铺名称',
    create_time      timestamp,
    create_user      bigint,
    last_modify_time timestamp,
    last_modify_user bigint,
    is_deleted       bigint
)
    comment '店铺信息'
    partitioned by (time_type tinyint, time_id varchar(10))
    stored as textfile;

drop table if exists s_item;
create table s_item
(
    id                bigint,
    shop_id           bigint comment '所属店铺id . fk shop.id',
    item_type         tinyint comment '商品类型 .\n0 . 简单类型，比如：书\n1 . 多规格类型：比如：衣服\n参考 : https://learnwoo.com/woocommerce-different-product-types/',
    item_name         varchar(45) comment '商品名称',
    category_one_id   bigint comment '一级分类 . fk item_category.id',
    category_two_id   bigint comment '二级分类 . fk item_category.id',
    category_three_id bigint comment '三级分类 . fk item_category.id',
    create_time       timestamp,
    create_user       bigint,
    last_modify_time  timestamp,
    last_modify_user  bigint,
    is_deleted        bigint
) comment '商品'
    partitioned by (time_type tinyint, time_id varchar(10))
    stored as textfile;

drop table if exists s_warehouse;
create table s_warehouse
(
    id               bigint,
    user_id          bigint comment '所属用户id',
    shop_id          bigint comment '所属店铺id',
    warehouse_type   tinyint comment '综合仓类型 . \n\n1. virtual . 没有实际仓库\n2. solid . 实体仓库 . 没错 ,　就是这个单词 , 从美剧中学的 .  ',
    warehouse_name   varchar(45) comment '仓库名称',
    create_time      timestamp,
    create_user      bigint,
    last_modify_time timestamp,
    last_modify_user bigint,
    is_deleted       bigint
)
    comment '仓库. 代表存放商品的仓库.'
    partitioned by (time_type tinyint, time_id varchar(10))
    stored as textfile;


use inte;
drop table if exists i_item;
create table i_item
(
    item_id           bigint,
    user_id           bigint,
    shop_id           bigint,
    warehouse_id      bigint,
    item_type         smallint comment '商品类型',
    category_one_id   bigint comment '一级分类',
    category_two_id   bigint comment '二级分类',
    category_three_id bigint comment '三级分类',
    warehouse_type    smallint comment '综合仓类型',
    mobile_phone      int comment '手机',
    email             varchar(45) comment '邮箱'
) comment '商品集成表'
    partitioned by (time_type tinyint, time_id varchar(10))
    stored as parquet;


use dm;
drop table if exists m_item_type;
create table m_item_type
(
    item_type    tinyint comment '商品类型',
    user_no      int comment '商家数',
    item_no      int comment '商品数',
    shop_no      int comment '店铺数',
    warehouse_no int comment '仓库数'
) comment '商品类型集市表'
    partitioned by (time_type tinyint, time_id varchar(10))
    stored as parquet;
