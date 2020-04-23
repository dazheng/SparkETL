package etl.utils;

import com.moandjiezana.toml.Toml;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;
import java.util.Map;
import java.util.function.BiConsumer;

/**
 * 从数据库中抽取数据
 */
public class Extract extends ETL {
    private final Logger logger = LoggerFactory.getLogger(Extract.class);
    private final SparkSession spark;
    private final DB db;

    public Extract(SparkSession spark, Integer timeType, String timeID, Integer backDate, String dbID, Integer frequency) throws Exception {
        super(spark, timeType, timeID, backDate, frequency);
        this.spark = spark;
        this.db = getDB(dbID);
        if (this.db == null) {
            return;
        }
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

    public void release() throws Exception {
        this.db.release();
    }

    public static String getExtractSQLDirectory() {
        return "extract/";
    }

    /**
     * 执行简单的插入，通过jdbc接口方式将SQL执行结果保存到hive中
     *
     * @param insertSql 插入hive表的语句
     * @param querySql  查询Rdb的语句
     */
    private void exeSQL(String insertSql, String querySql) {
        this.db.read(this.spark, querySql);
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
            exeSQLs(sqls, this::exeSQL, 1);
        } else if ("load".equals(exeType)) {
            exeSQLs(sqls, Public.rethrowBiConsumer(this::exeLoadSQL), 2);
        } else {
            this.logger.error("not support {}", exeType);
            throw new Exception("not support " + exeType);
        }
    }

    /**
     * 将Rdb查询结果保存成文件，然后以load方式入hive
     *
     * @param insertSQL 插入hive的SQL语句
     * @param sql       查询Rdb的SQL语句
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
        String table = Public.getTableFromInsertSQL(insertSQL);
        if (table == null) {
            return;
        }
        String fileName = Public.getTableDataDirectory(table, timeType) + table + ".txt";
//        RdbLoad(sql, fileName);
        BiConsumer<String, String> func = this.db.getExport();
        func.accept(sql, fileName);
        Public.printDuration(start, LocalDateTime.now());
        this.hiveLoad(insertSQL, fileName);
    }
}
