-- 从hive读数据，然后写入NoSQL中
@individual_user2
select id,
       user_name,
       password,
       mobile_phone,
       email,
       status,
       create_time,
       create_user,
       last_modify_time,
       last_modify_user,
       is_deleted
from stg.s_user where time_type=${time_type} and time_id='${time_id}';

@shop
select id,user_id,shop_name,create_time,create_user,last_modify_time,last_modify_user,is_deleted
from stg.s_shop where time_type=${time_type} and time_id='${time_id}';

@item;
select id,shop_id,item_type,item_name,category_one_id,category_two_id,category_three_id,create_time,create_user,last_modify_time,last_modify_user,is_deleted
from stg.s_item where time_type=${time_type} and time_id='${time_id}';

@warehouse
select id,user_id,shop_id,warehouse_type,warehouse_name,create_time,create_user,last_modify_time,last_modify_user,is_deleted
from stg.s_warehouse where time_type=${time_type} and time_id='${time_id}';

