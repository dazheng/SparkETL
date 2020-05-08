-- 从hive读数据，然后写入NoSQL中
@individual_user
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
       is_deleted -- Rdb中执行，Rdb语法，与上一行必须在不同行
from stg.s_user;


