package etl.utils;

import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.time.LocalDateTime;

/**
 * 从RDB中抽取数据
 */
public class Extract extends ETL {
    private final Logger logger = LoggerFactory.getLogger(Extract.class);
    private final SparkSession spark;
    private final RDB db;

    public Extract(SparkSession spark, Integer timeType, String timeID, Integer backDate, String dbID, Integer frequency) throws SQLException, ClassNotFoundException {
        super(spark, timeType, timeID, backDate, frequency);
        this.spark = spark;
        this.db = new RDB(dbID);
    }

    public void release() throws SQLException {
        this.db.release();
    }

    public static String getExtractSQLDirectory() {
        return "extract/";
    }

    /**
     * rdb的SQL生成v_table的临时视图
     *
     * @param table 表名
     * @param sql   执行的SQL
     */
    private void exeViewSQL(String table, String sql) {
        this.db.sqlToSpecialView(this.spark, table, sql);
    }

    /**
     * 将执行的N个sql生成指定的临时视图
     *
     * @param sqls n个SQL语句内容
     * @throws Exception
     */
    public void exeViewSQLs(String sqls) throws Exception {
        exeSQLs(sqls, this::exeViewSQL, 2);
    }

    /**
     * 执行简单的插入，通过jdbc接口方式将SQL执行结果保存到hive中
     *
     * @param insertSql 插入hive表的语句
     * @param querySql  查询rdb的语句
     */
    private void exeInsertSQL(String insertSql, String querySql) {
        this.db.sqlToView(this.spark, querySql);
        insertSql = insertSql + " select * from v_tmp";
        exeSQL(insertSql);
    }

    /**
     * 根据不同类型，以不同的方式执行SQL文件内容
     *
     * @param fileName sql文件名
     * @param exeType  执行类型
     * @throws Exception
     */
    protected void exeSQLFile(String fileName, String exeType) throws Exception {
        String sqls = Public.readSqlFile(getExtractSQLDirectory() + fileName);
        exeType = exeType.toLowerCase();
        if ("insert".equals(exeType)) {
            exeSQLs(sqls, this::exeInsertSQL, 2);
        } else if ("load".equals(exeType)) {
            exeSQLs(sqls, Public.rethrowBiConsumer(this::exeLoadSQL), 2);
        } else {
            this.logger.error("not support {}", exeType);
            throw new Exception("not support " + exeType);
        }
    }

    /**
     * 将rdb查询结果保存成文件，然后以load方式入hive
     *
     * @param insertSQL 插入hive的SQL语句
     * @param sql       查询rdb的SQL语句
     * @throws Exception
     */
    private void exeLoadSQL(String insertSQL, String sql) throws Exception {
        LocalDateTime start = LocalDateTime.now();
        if (insertSQL != null && !insertSQL.trim().isEmpty()) {
            this.logger.debug(Public.getMinusSep());
            this.logger.debug(insertSQL);
        }

        // 从insertSQL中分离出table,time_type；如果没有time_type，取默认1
        String timeType = String.valueOf(this.getTimeType());
        assert insertSQL != null;
        insertSQL = insertSQL.toLowerCase();
        String table = getTableFromSQL(insertSQL);
        if (table == null) {
            return;
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
            case "sqlserver":
                this.db.SQLServerExport(sql, fileName, table);
                break;
            case "postgresql":
                this.db.PostgreSQLExport(sql, fileName);
                break;
            case "db2":
                this.db.DB2Export(sql, fileName);
                break;
            default:
                this.logger.error("not support {}", this.db.getDbType());
                throw new Exception("not support " + this.db.getDbType());
        }
        Public.printDuration(start, LocalDateTime.now());
        this.hiveLoad(insertSQL, fileName);
    }

    /**
     * 从插入的SQL中获取目标表名
     *
     * @param insertSQL 插入SQL
     * @return 目标表名
     */
    private String getTableFromSQL(String insertSQL) {
        String[] sqls = insertSQL.split(" ");
        for (int i = 0; i < sqls.length; i++) {
            if ("table".equals(sqls[i])) {
                if (!sqls[i + 1].equals(" ")) {
                    return sqls[i + 1];
                }
            }
        }
        return null;
    }

}
