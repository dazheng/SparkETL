package etl.pub;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class Extract extends ETL {
    private final Logger logger = LogManager.getLogger();
    private final SparkSession session;
    private final DB db;

    public Extract(SparkSession session, Integer timeType, String timeID, Integer backDate, String dbID, Integer frequency) {
        super(session, timeType, timeID, backDate, frequency);
        this.session = session;
        this.db = new DB(dbID);
    }

    public void release() {
        this.db.release();
    }

    private void exeViewSql(String table, String sql) {
        this.db.sqlSpecialView(this.session, table, sql);
    }

    public void exeViewSqls(String sqls) {
        exeSqls(sqls, this::exeViewSql, 2);
    }

    private void exeExtractRdbmsSql(String insertSql, String querySql) {
        this.db.sqlView(this.session, querySql);
        insertSql = insertSql + " select * from v_tmp";
        exeSql(insertSql);
    }

    protected void exeExtractSqls(String sqls) {
        exeSqls(sqls, this::exeExtractRdbmsSql, 2);
    }

    private void exeExtractDirSql(String dir, String sql) {
        if (dir != null) {
            logger.info(dir);
        }
        Dataset<Row> df = db.sqlDF(session, sql);
        if (dir != null) {
            toLocalDir(df, dir);
        }
    }


    public void exeExtractDirSqlString(String sqls) {
        exeSqls(sqls, this::exeExtractDirSql, 4);
    }
}
