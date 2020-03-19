insert overwrite table inte.i_item partition (time_type=${time_type}, time_id='${time_id}')
select i.id, u.id, s.id, w.id, i.item_type,
    i.category_one_id, i.category_two_id, i.category_three_id, w.warehouse_type, u.mobile_phone, u.email
from stg.s_user u, stg.s_shop s, stg.s_item i, stg.s_warehouse w
where u.id = s.user_id and s.id = i.shop_id and s.id = w.shop_id
and u.time_type = ${time_type} and u.time_id ='${time_id}'
and s.time_type = ${time_type} and s.time_id ='${time_id}'
and i.time_type = ${time_type} and i.time_id ='${time_id}'
and w.time_type = ${time_type} and w.time_id ='${time_id}';
