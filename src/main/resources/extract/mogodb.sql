insert overwrite table stg.log partition(time_type=${time_type}, time_id=${time_id}) -- 单独一行，spark中执行
select  -- 单独一行，查询的mongodb字段
from collection1 -- 单独一行，collection
where {"create_time":{"$gte": "${date_start}", "$lt":"${date_end}"}}; -- 单独一行，find条件，json表达式
