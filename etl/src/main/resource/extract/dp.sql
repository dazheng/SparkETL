insert overwrite table dim.d_index
select index_id,ch_name,en_name,index_type,is_valid,dsc,update_time from d_index where is_valid = 1;
