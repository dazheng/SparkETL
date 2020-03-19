package etl.utils;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class Export extends ETL {
    private final Logger logger = LoggerFactory.getLogger(Export.class);
    private final RDB db;

    public Export(SparkSession spark, Integer timeType, String timeID, Integer backDate, String dbID, Integer frequency) {
        super(spark, timeType, timeID, backDate, frequency);
        this.db = new RDB(dbID);
    }

    public void release() {
        this.db.release();
    }

    public static String getExportSqlDir() {
        return "export/";
    }

    private void deleteRDBTable(String table) {
        String start = getStartTimeID();
        String end = getEndTimeID();
        String sql = String.format("delete from %s where time_type = %s and time_id between '%s' and '%s'", table, getTimeType(), start, end);
        this.db.exeSQL(sql);
    }

    private void sql2RDBTable(String table, String sql) {
        Dataset<Row> df = exeSQL(sql);
        this.db.jdbcWrite(df, table);
    }

    private void toRDBTableIncreate(String table, String sql) {
        deleteRDBTable(table);
        sql2RDBTable(table, sql);
    }

    private void toRDBTableFull(String table, String sql) {
        this.db.exeSQL("truncate table " + table);
        sql2RDBTable(table, sql);
    }

    private void exeRDBSQL(String dir, String sql) {
        this.db.exeSQL(sql);
    }

    private void exeInsertSQL(String table, String sql) {
        if (table == null || table.isEmpty()) {
            this.db.exeSQL(sql);
        } else {
            sql2RDBTable(table, sql);
        }
    }

    private void  exeLoadSQL(String table, String sql) {
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
                default:
                    logger.error("not support {}", this.db.getDbType());
            }
        }
    }

    protected void exeSQLFile(String fileName, String exeType) {
        String sqls = Public.readSqlFile(getExportSqlDir() + fileName);
        exeType = exeType.toLowerCase();
        switch (exeType) {
            case "insert":
                exeSQLs(sqls, this::exeInsertSQL, 3);
                break;
            case "export":
                exeSQLs(sqls, this::exeLoadSQL, 3);
                break;
            case "db":
                exeSQLs(sqls, this::exeRDBSQL, 1);
                break;
        }
    }


    private Map<String, String> RDBTableInfo(@NotNull String table) {
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

    public void simpleToRDBIncrease(@NotNull List<String> tables) {
        for (String table : tables) {
            Map<String, String> paras = RDBTableInfo(table);
            assert paras != null;
            String nativeTable = paras.get("native_table");
            String cs = paras.get("columns");
            String start = getStartTimeID();
            String end = getEndTimeID();
            String sql = "select " + cs + " from " + table + " where time_type = " + getTimeType() + " and time_id between '" + start + "' and '" + end + "'";
            toRDBTableIncreate(nativeTable, sql);
        }
    }

    public void simpleToRDBFull(@NotNull List<String> tables) {
        for (String table : tables) {
            Map<String, String> paras = RDBTableInfo(table);
            assert paras != null;
            String nativeTable = paras.get("native_table");
            String cs = paras.get("columns");
            String sql = "select " + cs + " from " + table;
            toRDBTableFull(nativeTable, sql);
        }
    }
}
