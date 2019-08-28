refresh table stg.s_pay_flow;
insert overwrite table inte.i_order partition(time_id)
select client_id,biz_date,row_id,server_time,branch_id,oper_date,flow_no,flow_id,vip_no,pay_way,
    pay_amount,sell_way,cashiers_no,current_timestamp create_time, time_id
from(select client_id,biz_date,CONCAT_WS('|',cast(client_id as string),branch_no,flow_no,flow_id) as row_id,server_time,branch_no branch_id,oper_date,flow_no,flow_id,vip_no,pay_way,
    pay_amount,sell_way,cashiers_no,current_timestamp create_time, time_id,
    row_number() over(partition by CONCAT_WS('|',cast(client_id as string),branch_no,flow_no,flow_id) order by server_time desc) rn
from stg.s_pay_flow
where time_id between '${v_start_time_id}' and '${v_time_id}'
    and pay_amount between -100000 and 100000
)a where rn = 1;
