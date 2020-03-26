-- mysql --
create table if not exists individual_user
(
    id               bigint unsigned  not null,
    username         varchar(45)      not null comment '用户名',
    password         varchar(45)      not null comment '密码',
    mobile_phone     int unsigned     not null default 0 comment '手机',
    email            varchar(45)      not null default '' comment '邮箱',
    status           tinyint unsigned not null default 1 comment '状态. \n1 : 正常',
    create_time      datetime         not null default current_timestamp,
    create_user      bigint unsigned  not null,
    last_modify_time datetime         not null default current_timestamp on update current_timestamp,
    last_modify_user bigint unsigned  not null,
    is_deleted       bigint unsigned  not null default 0,
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
    shop_name        varchar(45)     not null comment '店铺名称',
    create_time      datetime        not null default current_timestamp,
    create_user      bigint unsigned not null,
    last_modify_time datetime        not null default current_timestamp on update current_timestamp,
    last_modify_user bigint unsigned not null,
    is_deleted       bigint unsigned not null default 0,
    primary key (id),
    index fk_user_id_idx (user_id asc)
)
    comment = '店铺信息';


create table if not exists item
(
    id                bigint unsigned  not null,
    shop_id           bigint unsigned  not null comment '所属店铺id . fk shop.id',
    item_type         tinyint unsigned not null default 1 comment '商品类型 .\n0 . 简单类型，比如：书\n1 . 多规格类型：比如：衣服\n参考 : https://learnwoo.com/woocommerce-different-product-types/',
    item_name         varchar(45)      not null comment '商品名称',
    category_one_id   bigint unsigned  null comment '一级分类 . fk item_category.id',
    category_two_id   bigint unsigned  null comment '二级分类 . fk item_category.id',
    category_three_id bigint unsigned  null comment '三级分类 . fk item_category.id',
    create_time       datetime         not null default current_timestamp,
    create_user       bigint unsigned  not null,
    last_modify_time  datetime         not null default current_timestamp on update current_timestamp,
    last_modify_user  bigint unsigned  not null,
    is_deleted        bigint unsigned  not null default 0,
    primary key (id),
    index fk_shop_id_idx (shop_id asc),
    index fk_category_one_id_idx (category_one_id asc),
    index fk_category_two_id_idx (category_two_id asc),
    index fk_category_three_id_idx (category_three_id asc)
)
    comment = '商品';


create table if not exists warehouse
(
    id               bigint unsigned  not null,
    user_id          bigint unsigned  not null comment '所属用户id',
    shop_id          bigint unsigned  not null comment '所属店铺id',
    warehouse_type   tinyint unsigned not null default 1 comment '综合仓类型 . \n\n1. virtual . 没有实际仓库\n2. solid . 实体仓库 . 没错 ,　就是这个单词 , 从美剧中学的 .  ',
    warehouse_name   varchar(45)      not null comment '仓库名称',
    create_time      datetime         not null default current_timestamp,
    create_user      bigint unsigned  not null,
    last_modify_time datetime         not null default current_timestamp on update current_timestamp,
    last_modify_user bigint unsigned  not null,
    is_deleted       bigint unsigned  not null default 0,
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
    warehouse_no int         not null comment '仓库数',
    primary key (time_type, time_id, item_type)
) comment '商品类型集市表';

