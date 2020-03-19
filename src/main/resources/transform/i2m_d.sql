insert overwrite table dm.m_item_type partition (time_type=${time_type}, time_id='${time_id}')
select coalesce(item_type,0) item_type, count(distinct user_id) user_no, count(item_id) item_no, count(distinct shop_id) shop_no,
       count(distinct warehouse_id) warehouse_no
from inte.i_item
where time_type = ${time_type} and time_id = '${time_id}'
group by coalesce(item_type,0) ;
