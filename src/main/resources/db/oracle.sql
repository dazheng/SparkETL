drop table individual_user;
create table individual_user
(
    id               number(38, 0) not null,
    username         varchar2(135) not null,
    password         varchar2(135) not null,
    mobile_phone     number(38, 0) default 0,
    email            varchar2(135) default '',
    status           number(8, 0)  default 1,
    create_time      date          default current_date,
    create_user      number(38, 0) not null,
    last_modify_time date          default current_date,
    last_modify_user number(38, 0) not null,
    is_deleted       number(38, 0) default 0,
    primary key (id)
);
comment on table individual_user is '个人用户－基本信息';
comment on column individual_user.username is '用户名';
comment on column individual_user.mobile_phone is '密码';
comment on column individual_user.email is '邮箱';
comment on column individual_user.email is '状态. \n1 : 正常';

create unique index idx_user_mobile_phone on individual_user (mobile_phone asc);
create unique index idx_user_email on individual_user (email asc);
create unique index idx_user_username on individual_user (username asc);


drop table shop;
create table shop
(
    id               number(38, 0) not null,
    user_id          number(38, 0) not null,
    shop_name        varchar2(135) not null,
    create_time      date          default current_date,
    create_user      number(38, 0) not null,
    last_modify_time date          default current_date,
    last_modify_user number(38, 0) not null,
    is_deleted       number(38, 0) default 0,
    primary key (id)
);
comment on table shop is '店铺信息';
comment on column shop.user_id is '所属用户id';
comment on column shop.shop_name is '店铺名称';

create index idx_shop_user_id on shop (user_id asc);

drop table item;
create table item
(
    id                number(38, 0) not null,
    shop_id           number(38, 0) not null,
    item_type         number(8, 0)  default 1,
    item_name         varchar2(135) not null,
    category_one_id   number(38, 0) null,
    category_two_id   number(38, 0) null,
    category_three_id number(38, 0) null,
    create_time       date          default current_date,
    create_user       number(38, 0) not null,
    last_modify_time  date          default current_date,
    last_modify_user  number(38, 0) not null,
    is_deleted        number(38, 0) default 0,
    primary key (id)
);
comment on table item is '商品';
comment on column item.shop_id is '所属店铺id . fk shop.id';
comment on column item.item_type is '商品类型 .\n0 . 简单类型，比如：书\n1 . 多规格类型：比如：衣服\n参考 : https://learnwoo.com/woocommerce-different-product-types/';
comment on column item.item_name is '商品名称';
comment on column item.category_one_id is '一级分类';
comment on column item.category_two_id is '二级分类';
comment on column item.category_three_id is '三级分类';

create index fk_item_shop_id on item (shop_id asc);
create index fk_item_category_one_id on item (category_one_id asc);
create index fk_item_category_two_id on item (category_two_id asc);
create index fk_item_category_three_id on item (category_three_id asc);

drop table warehouse;
create table warehouse
(
    id               number(38, 0) not null,
    user_id          number(38, 0) not null,
    shop_id          number(38, 0) not null,
    warehouse_type   number(8, 0)  default 1,
    warehouse_name   varchar2(135) not null,
    create_time      date          default current_date,
    create_user      number(38, 0) not null,
    last_modify_time date          default current_date,
    last_modify_user number(38, 0) not null,
    is_deleted       number(38, 0) default 0,
    primary key (id)
);
comment on table warehouse is '仓库. 代表存放商品的仓库';
comment on column warehouse.user_id is '所属用户id';
comment on column warehouse.shop_id is '所属店铺id';
comment on column warehouse.warehouse_type is '综合仓类型 . \n\n1. virtual . 没有实际仓库\n2. solid . 实体仓库';
comment on column warehouse.warehouse_name is '仓库名称';

create index idx_warehouse_user_id on warehouse (user_id asc);
create index idx_warehouse_shop_id on warehouse (shop_id asc);

drop table m_item_type;
create table m_item_type
(
    time_type    number(8, 0)  not null,
    time_id      varchar2(10)  not null,
    item_type    number(8, 0)  not null,
    user_no      number(38, 0) not null,
    item_no      number(38, 0) not null,
    shop_no      number(38, 0) not null,
    warehouse_no number(38, 0) not null,
    primary key (time_type, time_id, item_type)
);
comment on table m_item_type is '商品类型集市表';
comment on column m_item_type.time_type is '时间类型';
comment on column m_item_type.time_id is '时间id';
comment on column m_item_type.item_type is '商品类型';
comment on column m_item_type.user_no is '商家数';
comment on column m_item_type.item_no is '商品数';
comment on column m_item_type.shop_no is '店铺数';
comment on column m_item_type.warehouse_no is '仓库数';

