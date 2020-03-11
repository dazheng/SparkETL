insert overwrite table dm.m_holiday
select holiday_type, count(1)
from stg.s_holiday
group by holiday_type;
