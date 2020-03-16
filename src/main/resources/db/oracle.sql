create or replace procedure sql_to_csv(p_query in varchar2, -- plsql文
                                       p_dir in varchar2, -- 导出的文件放置目录
                                       p_filename in varchar2 -- csv名
)
    is
    l_output       utl_file.file_type;
    l_thecursor    integer default dbms_sql.open_cursor;
    l_columnvalue  varchar2(4000);
    l_status       integer;
    l_colcnt       number := 0;
    l_separator    varchar2(1);
    l_desctbl      dbms_sql.desc_tab;
    p_max_linesize number := 32000;
begin
    --open file
    l_output := utl_file.fopen(p_dir, p_filename, 'w', p_max_linesize);
    --define date format
    execute immediate 'alter session set nls_date_format=''yyyy-mm-dd hh24:mi:ss''';
    --open cursor
    dbms_sql.parse(l_thecursor, p_query, dbms_sql.native);
    dbms_sql.describe_columns(l_thecursor, l_colcnt, l_desctbl);
    --dump table column name
    for i in 1 .. l_colcnt
        loop
            utl_file.put(l_output, l_separator || '"' || l_desctbl(i).col_name || '"'); --输出表字段
            dbms_sql.define_column(l_thecursor, i, l_columnvalue, 4000);
            l_separator := ',';
        end loop;
    utl_file.new_line(l_output);
    --输出表字段
    --execute the query statement
    l_status := dbms_sql.execute(l_thecursor);

    --dump table column value
    while (dbms_sql.fetch_rows(l_thecursor) > 0)
        loop
            l_separator := '';
            for i in 1 .. l_colcnt
                loop
                    dbms_sql.column_value(l_thecursor, i, l_columnvalue);
                    utl_file.put(l_output,
                                 l_separator || '"' ||
                                 trim(both ' ' from replace(l_columnvalue, '"', '""')) || '"');
                    l_separator := ',';
                end loop;
            utl_file.new_line(l_output);
        end loop;
    --close cursor
    dbms_sql.close_cursor(l_thecursor);
    --close file
    utl_file.fclose(l_output);
exception
    when others then
        raise;
end;
