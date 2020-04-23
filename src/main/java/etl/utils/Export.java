package etl.utils;

import com.moandjiezana.toml.Toml;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;


/**
 * 将spark计算结果同步到数据库中
 */
public class Export extends ETL {
    private final Logger logger = LoggerFactory.getLogger(Export.class);
    private final DB db;

    public Export(SparkSession spark, Integer timeType, String timeID, Integer backDate, String dbID, Integer frequency) throws Exception {
        super(spark, timeType, timeID, backDate, frequency);
        this.db = getDB(dbID);
    }

    private DB getDB(String dbID) throws Exception {
        Map<String, Object> map = Public.getDB(dbID);
        if (map != null) {
            String type = (String) map.get("type");
            Toml tdb = (Toml) map.get("db");
            DBFactory dbf = new DBFactory();
            return dbf.produce(type, tdb);
        }
        return null;
    }

    /**
     * 释放申请的资源，目前是只有数据库连接
     *
     * @throws SQLException
     */
    public void release() throws Exception {
        this.db.release();
    }

    public static String getExportSqlDir() {
        return "export/";
    }

    /**
     * 删除Rdb的数据
     *
     * @param table Rdb表名
     * @throws SQLException
     */
    private void deleteRdbTable(String table) throws Exception {
        String start = getStartTimeID();
        String end = getEndTimeID();
        String sql = String.format("delete from %s where time_type = %s and time_id between '%s' and '%s'", table, getTimeType(), start, end);
        this.db.exeSQL(sql);
    }

    /**
     * SQL结果保存成Rdb表
     *
     * @param table
     * @param sql
     */
    private void sqlToRdbTable(String table, String sql) {
        Dataset<Row> df = exeSQL(sql);
        this.db.write(df, table);
    }

    /**
     * 先删除增量结果，然后将sql结果保存到Rdb的表中
     *
     * @param table Rdb表名
     * @param sql   执行的sql语句
     * @throws SQLException
     */
    private void toRdbTableIncreate(String table, String sql) throws Exception {
        deleteRdbTable(table);
        sqlToRdbTable(table, sql);
    }

    /**
     * 先删除整个表，然后将sql结果保存到Rdb的表中
     *
     * @param table
     * @param sql
     * @throws SQLException
     */
    private void toRdbTableFull(String table, String sql) throws Exception {
        this.db.exeSQL("truncate table " + table);
        sqlToRdbTable(table, sql);
    }

    /**
     * 执行Rdb中的SQL
     *
     * @param insertSQL 插入的SQL
     * @param sql       要执行的SQL
     * @throws SQLException
     */
    private void exeRdbSQL(String insertSQL, String sql) throws Exception {
        this.db.exeSQL(sql);
    }

    /**
     * sql查询结果同步到表中
     *
     * @param table
     * @param sql
     * @throws SQLException
     */
    private void exeInsertSQL(String table, String sql) throws Exception {
        if (table == null || table.isEmpty()) {
            this.db.exeSQL(sql);
        } else {
            sqlToRdbTable(table, sql);
        }
    }

    /**
     * 将sql结果生成本地文件，然后以load方式入Rdb库
     *
     * @param table 表名
     * @param sql   要执行的SQL
     * @throws SQLException
     * @throws IOException
     * @throws InterruptedException
     */
    private void exeLoadSQL(String table, String sql) throws Exception {
        if (table == null || table.isEmpty()) {
            this.db.exeSQL(sql);
        } else {
            Dataset<Row> df = exeSQL(sql);
            String frequency = String.valueOf(getFrequency());
            toLocalDirectory(df, Public.getTableDataDirectory(table, frequency));
            List<String> files = this.db.getLoadFiles(table, frequency);
            if (files.isEmpty()) {
                return;
            }
//            RdbLoad(table, files, df);
            BiConsumer<String, List<String>> func = this.db.getLoad();
            func.accept(table, files);
        }
    }

    /**
     * 读取sql文件，以不同的方式将数据同步到Rdb中
     *
     * @param fileName
     * @param exeType
     * @throws Exception
     */
    protected void exeSQLFile(String fileName, String exeType) throws Exception {
        String sqls = Public.readSqlFile(getExportSqlDir() + fileName);
        exeType = exeType.toLowerCase();
        switch (exeType) {
            case "insert":
                exeSQLs(sqls, Public.rethrowBiConsumer(this::exeInsertSQL), 3);
                break;
            case "load":
                exeSQLs(sqls, Public.rethrowBiConsumer(this::exeLoadSQL), 3);
                break;
            case "db":
                exeSQLs(sqls, Public.rethrowBiConsumer(this::exeRdbSQL), 1);
                break;
            default:
                this.logger.error("not suport {}", exeType);
        }
    }


    /**
     * 获取目标表的字段
     *
     * @param table 表名
     * @return 表名，以逗号分隔的列名字符串
     * @throws SQLException
     */
    private Map<String, String> getRdbTableColumns(@NotNull String table) throws Exception {
        String[] nt = table.split(".");
        String nativeTable = nt.length == 2 ? nt[1] : table;
        String cs = this.db.getTableColumns(nativeTable);
        if (cs.length() == 0) {
            return null;
        }
        Map<String, String> map = new HashMap<String, String>();
        map.put("native_table", nativeTable);
        map.put("columns", cs);
        return map;
    }

    /**
     * 将表名中的内容，增量方式同步到Rdb中。需要原表、目标表的表名、字段名、字段类型完全一致
     *
     * @param tables 表名列表
     * @throws SQLException
     */
    public void simpleToRdbIncrease(@NotNull List<String> tables) throws Exception {
        for (String table : tables) {
            Map<String, String> paras = getRdbTableColumns(table);
            assert paras != null;
            String nativeTable = paras.get("native_table");
            String cs = paras.get("columns");
            String start = getStartTimeID();
            String end = getEndTimeID();
            String sql = "select " + cs + " from " + table + " where time_type = " + getTimeType() + " and time_id between '" + start + "' and '" + end + "'";
            toRdbTableIncreate(nativeTable, sql);
        }
    }

    /**
     * 将表名中的内容，全量方式同步到Rdb中。需要原表、目标表的表名、字段名、字段类型完全一致
     *
     * @param tables 表名列表
     * @throws SQLException
     */
    public void simpleToRdbFull(@NotNull List<String> tables) throws Exception {
        for (String table : tables) {
            Map<String, String> paras = getRdbTableColumns(table);
            assert paras != null;
            String nativeTable = paras.get("native_table");
            String cs = paras.get("columns");
            String sql = "select " + cs + " from " + table;
            toRdbTableFull(nativeTable, sql);
        }
    }
}
