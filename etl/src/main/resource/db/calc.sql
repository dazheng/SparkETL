insert overwrite table m_holiday
select holiday_type, count(1)
from s_holiday
group by holiday_type;
