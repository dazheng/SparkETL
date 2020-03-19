package etl.utils;

import org.apache.spark.sql.SparkSession;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class Extract extends ETL {
    private final Logger logger = LoggerFactory.getLogger(Extract.class);
    private final SparkSession spark;
    private final RDB db;

    public Extract(SparkSession spark, Integer timeType, String timeID, Integer backDate, String dbID, Integer frequency) {
        super(spark, timeType, timeID, backDate, frequency);
        this.spark = spark;
        this.db = new RDB(dbID);
    }

    public void release() {
        this.db.release();
    }

    public static String getExtractSQLDirectory() {
        return "extract/";
    }

    private void exeViewSQL(String table, String sql) {
        this.db.sqlToSpecialView(this.spark, table, sql);
    }

    public void exeViewSQLs(String sqls) {
        exeSQLs(sqls, this::exeViewSQL, 2);
    }

    private void exeInsertSQL(String insertSql, String querySql) {
        this.db.sqlToView(this.spark, querySql);
        insertSql = insertSql + " select * from v_tmp";
        exeSQL(insertSql);
    }

    protected void exeSQLFile(String fileName, String exeType) {
        String sqls = Public.readSqlFile(getExtractSQLDirectory() + fileName);
        exeType = exeType.toLowerCase();
        if (exeType.equals("insert")) {
            exeSQLs(sqls, this::exeInsertSQL, 2);
        } else if (exeType.equals("import")) {
            exeSQLs(sqls, this::exeImportSQL, 2);
        }
    }

    private void exeImportSQL(String insertSQL, String sql) {
        if (insertSQL != null && !insertSQL.trim().isEmpty()) {
            logger.info(insertSQL);
        }

        // 从insertSQL中分离出table,time_type；如果没有time_type，取默认1
        String timeType = "1";
        assert insertSQL != null;
        insertSQL = insertSQL.toLowerCase();
        String table = getTableFromSQL(insertSQL);
        if (table == null) {
            return;
        }
        String tt = getTimeTypeFromSQL(insertSQL);
        if (tt != null) {
            timeType = tt;
        }
        String fileName = Public.getTableDataDirectory(table, timeType) + table + ".txt";

        // 分数据库
        switch (this.db.getDbType()) {
            case "mysql":
                this.db.MysqlExport(sql, fileName);
                break;
            case "oracle":
                this.db.OracleExport(sql, fileName);
                break;
            default:
                logger.error("not support {}", this.db.getDbType());
        }
        this.hiveLoad(insertSQL, fileName);
    }

    private String getTableFromSQL(String insertSQL) {
        String[] sqls = insertSQL.split(" ");
        for (int i = 0; i < sqls.length; i++) {
            if (sqls[i].equals("table")) {
                if (!sqls[i + 1].equals(" ")) {
                    return sqls[i + 1];
                }
            }
        }
        return null;
    }

    private String getTimeTypeFromSQL(String insertSQL) {
        String s = "\\("; // "("
        String[] sqls = insertSQL.split(s);
        for (String sql : sqls) {
            int idx = sql.indexOf("time_type");
            if (idx != -1) {
                String[] ss = sql.substring(idx).split("=");
                if (ss.length >= 2) {
                    return ss[1].substring(0, ss[1].indexOf(",") - 1).trim(); // TODO：测试
                }
            }
        }
        return null;
    }
}
