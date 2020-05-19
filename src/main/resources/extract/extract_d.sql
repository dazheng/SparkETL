insert overwrite table stg.s_user partition(time_type=${time_type}, time_id='${time_id}')   -- hive中执行，hive语法
select id,
       username,
       password,
       mobile_phone,
       email,
       status,
       create_time,
       create_user,
       last_modify_time,
       last_modify_user,
       is_deleted
from individual_user;

insert overwrite table stg.s_shop partition(time_type=${time_type}, time_id='${time_id}')
select id,user_id,shop_name,create_time,create_user,last_modify_time,last_modify_user,is_deleted
from shop;

insert overwrite table stg.s_item partition(time_type=${time_type}, time_id='${time_id}')
select id,shop_id,item_type,item_name,category_one_id,category_two_id,category_three_id,create_time,create_user,last_modify_time,last_modify_user,is_deleted
from item;

insert overwrite table stg.s_warehouse partition(time_type=${time_type}, time_id='${time_id}')
select id,user_id,shop_id,warehouse_type,warehouse_name,create_time,create_user,last_modify_time,last_modify_user,is_deleted
from warehouse;
