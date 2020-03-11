package etl.pub;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.jetbrains.annotations.NotNull;

import java.util.HashMap;
import java.util.Map;


public class Export extends ETL {
    private final Logger logger = LogManager.getLogger();
    private final DB db;

    public Export(SparkSession session, Integer timeType, String timeID, Integer backDate, String dbID, Integer frequency) {
        super(session, timeType, timeID, backDate, frequency);
        this.db = new DB(dbID);
    }

    public void release() {
        this.db.release();
    }

    private void deleteTable(String table) {
        String start = getStartTimeID();
        String end = getEndTimeID();
        String sql = String.format("delete from %s where time_type = %s and time_id between '%s' and '%s'", table, getTimeType(), start, end);
        this.db.exeSql(sql);
    }


    private void sql2Table(String table, String sql) {
        Dataset<Row> df = exeSql(sql);
        this.db.dfTable(df, table);
    }

    private void deleteInsertTable(String table, String delete_sql, String sql) {
        this.db.exeSql(delete_sql);
        sql2Table(table, sql);
    }

    private void toTableIncreate(String table, String sql) {
        deleteTable(table);
        sql2Table(table, sql);
    }

    private void toTableFull(String table, String sql) {
        this.db.exeSql("truncate table " + table);
        sql2Table(table, sql);
    }

    private void exeDBSql(String dir, String sql) {
        this.db.exeSql(sql);
    }

    protected void exeDBSqls(String sqls) {
        exeSqls(sqls, this::exeDBSql, 1);
    }

    private void exeExportRDBMSSql(String table, String sql) {
        if (table == null) {
            this.db.exeSql(sql);
        } else {
            sql2Table(table, sql);
        }
    }

    protected void exeSqls(String sqls) {
        exeSqls(sqls, this::exeExportRDBMSSql, 3);
    }

    private void exeLoadSql(String table, String sql) {
        try {
            if (table == null) {
                this.db.exeSql(sql);
            } else {
                Dataset<Row> df = exeSql(sql);
                toLocalDir(df, Func.getDataDir() + table + "/" + getFrequency() + "/");
                this.db.load(table, String.valueOf(getFrequency()));
            }
        } catch (Exception e) {
            logger.fatal(e);
        }
    }

    protected void exeLoadSqls(String sqls) {
        exeSqls(sqls, this::exeLoadSql, 3);
    }

    private Map<String, String> tableInfo(@NotNull String table) {
        String[] nt = table.split(".");
        String nativeTable = nt.length == 2 ? nt[1] : table;
        String cs = this.db.getMysqlColumn(nativeTable);
        if (cs.length() == 0) {
            return null;
        }
        Map<String, String> map = new HashMap<String, String>();
        map.put("native_table", nativeTable);
        map.put("columns", cs);
        return map;
    }

    public void simple2DBIncrease(@NotNull String table) {
        Map<String, String> paras = tableInfo(table);
        assert paras != null;
        String nativeTable = paras.get("native_table");
        String cs = paras.get("columns");
        String start = getStartTimeID();
        String end = getEndTimeID();
        String sql = "select " + cs + " from " + table + " where time_type = " + getTimeType() + " and time_id between '" + start + "' and '" + end + "'";
        toTableIncreate(nativeTable, sql);
    }

    public void simple2DBFull(String table) {
        Map<String, String> paras = tableInfo(table);
        assert paras != null;
        String nativeTable = paras.get("native_table");
        String cs = paras.get("columns");
        String sql = "select " + cs + " from " + table;
        toTableFull(nativeTable, sql);
    }

}
