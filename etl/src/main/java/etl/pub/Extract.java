package etl.pub;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Extract extends ETL {
    private final Logger logger = LoggerFactory.getLogger(Extract.class);
    private final SparkSession spark;
    private final DB db;

    public Extract(SparkSession spark, Integer timeType, String timeID, Integer backDate, String dbID, Integer frequency) {
        super(spark, timeType, timeID, backDate, frequency);
        this.spark = spark;
        this.db = new DB(dbID);
    }

    public void release() {
        this.db.release();
    }

    public static String getExtractSqlDir() {
        return "extract/";
    }

    private void exeViewSql(String table, String sql) {
        this.db.sqlSpecialView(this.spark, table, sql);
    }

    public void exeViewSqls(String sqls) {
        exeSQLs(sqls, this::exeViewSql, 2);
    }

    private void exeSQL(String insertSql, String querySql) {
        this.db.sqlView(this.spark, querySql);
        insertSql = insertSql + " select * from v_tmp";
        exeSQL(insertSql);
    }

    protected void exeSQLFile(String fileName, String exeType) {
        String sqls = Func.readSqlFile(getExtractSqlDir() + fileName);
        exeType = exeType.toLowerCase();
        if (exeType.equals("insert")) {
            exeSQLs(sqls, this::exeSQL, 2);
        } else if (exeType.equals("import")) {
            exeSQLs(sqls, this::exeImportSql, 4);
        }
    }

    private void exeImportSql(String dir, String sql) {
        if (dir != null && !dir.trim().isEmpty()) {
            logger.info(dir);
        }
        Dataset<Row> df = db.sqlDF(spark, sql);
        if (dir != null && !dir.trim().isEmpty()) {
            toLocalDir(df, dir);
        }
    }
}
