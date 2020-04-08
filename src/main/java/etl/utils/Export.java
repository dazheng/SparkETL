package etl.utils;

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


/**
 * 将spark结算结果同步到RDBMS中
 */
public class Export extends ETL {
    private final Logger logger = LoggerFactory.getLogger(Export.class);
    private final RDB db;

    public Export(SparkSession spark, Integer timeType, String timeID, Integer backDate, String dbID, Integer frequency) throws SQLException, ClassNotFoundException {
        super(spark, timeType, timeID, backDate, frequency);
        this.db = new RDB(dbID);
    }

    /**
     * 释放申请的资源，目前是只有数据库连接
     *
     * @throws SQLException
     */
    public void release() throws SQLException {
        this.db.release();
    }

    public static String getExportSqlDir() {
        return "export/";
    }

    /**
     * 删除RDB的数据
     *
     * @param table rdb表名
     * @throws SQLException
     */
    private void deleteRDBTable(String table) throws SQLException {
        String start = getStartTimeID();
        String end = getEndTimeID();
        String sql = String.format("delete from %s where time_type = %s and time_id between '%s' and '%s'", table, getTimeType(), start, end);
        this.db.exeSQL(sql);
    }

    /**
     * SQL结果保存成rdb表
     *
     * @param table
     * @param sql
     */
    private void sqlToRDBTable(String table, String sql) {
        Dataset<Row> df = exeSQL(sql);
        this.db.jdbcWrite(df, table);
    }

    /**
     * 先删除增量结果，然后将sql结果保存到rdb的表中
     *
     * @param table rdb表名
     * @param sql   执行的sql语句
     * @throws SQLException
     */
    private void toRDBTableIncreate(String table, String sql) throws SQLException {
        deleteRDBTable(table);
        sqlToRDBTable(table, sql);
    }

    /**
     * 先删除整个表，然后将sql结果保存到rdb的表中
     *
     * @param table
     * @param sql
     * @throws SQLException
     */
    private void toRDBTableFull(String table, String sql) throws SQLException {
        this.db.exeSQL("truncate table " + table);
        sqlToRDBTable(table, sql);
    }

    /**
     * 执行rdb中的SQL
     *
     * @param insertSQL 插入的SQL
     * @param sql       要执行的SQL
     * @throws SQLException
     */
    private void exeRDBSQL(String insertSQL, String sql) throws SQLException {
        this.db.exeSQL(sql);
    }

    /**
     * sql查询结果同步到表中
     *
     * @param table
     * @param sql
     * @throws SQLException
     */
    private void exeInsertSQL(String table, String sql) throws SQLException {
        if (table == null || table.isEmpty()) {
            this.db.exeSQL(sql);
        } else {
            sqlToRDBTable(table, sql);
        }
    }

    /**
     * 将sql结果生成本地文件，然后以load方式入RDB库
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

            switch (this.db.getDbType()) {
                case "mysql":
                    this.db.MySQLLoad(table, files);
                    break;
                case "oracle":
                    this.db.OracleLoad(table, files);
                    break;
                case "sqlserver":
                    this.db.SQLServerLoad(table, files);
                    break;
                case "postgresql":
//                    this.db.PostgreSQLLoad(table, files); // TODO：合适的laod方式
                    this.db.jdbcWrite(df, table);
                    break;
                case "db2":
                    this.db.DB2Load(table, files);
                    break;
                default:
                    this.logger.error("not support {}", this.db.getDbType());
                    throw new Exception("not support " + this.db.getDbType());
            }
        }
    }

    /**
     * 读取sql文件，以不同的方式将数据同步到RDB中
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
                exeSQLs(sqls, Public.rethrowBiConsumer(this::exeRDBSQL), 1);
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
    private Map<String, String> getRDBTableColumns(@NotNull String table) throws Exception {
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
     * 将表名中的内容，增量方式同步到RDB中。需要原表、目标表的表名、字段名、字段类型完全一致
     *
     * @param tables 表名列表
     * @throws SQLException
     */
    public void simpleToRDBIncrease(@NotNull List<String> tables) throws Exception {
        for (String table : tables) {
            Map<String, String> paras = getRDBTableColumns(table);
            assert paras != null;
            String nativeTable = paras.get("native_table");
            String cs = paras.get("columns");
            String start = getStartTimeID();
            String end = getEndTimeID();
            String sql = "select " + cs + " from " + table + " where time_type = " + getTimeType() + " and time_id between '" + start + "' and '" + end + "'";
            toRDBTableIncreate(nativeTable, sql);
        }
    }

    /**
     * 将表名中的内容，全量方式同步到RDB中。需要原表、目标表的表名、字段名、字段类型完全一致
     *
     * @param tables 表名列表
     * @throws SQLException
     */
    public void simpleToRDBFull(@NotNull List<String> tables) throws Exception {
        for (String table : tables) {
            Map<String, String> paras = getRDBTableColumns(table);
            assert paras != null;
            String nativeTable = paras.get("native_table");
            String cs = paras.get("columns");
            String sql = "select " + cs + " from " + table;
            toRDBTableFull(nativeTable, sql);
        }
    }
}
