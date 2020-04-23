package etl.utils;

import com.moandjiezana.toml.Toml;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.time.LocalDateTime;
import java.util.List;
import java.util.function.BiConsumer;

public class Redis implements DB {
    private final Logger logger = LoggerFactory.getLogger(Rdb.class);
    private final String dbType;
    private final String host;
    private final String port;
    private final String dbNO;
    private final String auth;


    Redis(Toml db) {
        assert db != null;
        this.dbType = Public.DB_REDIS;
        this.host = db.getString("host");
        this.port = db.getString("port", "6379");
        this.dbNO = db.getString("dbNO", "0");
        this.auth = db.getString("auth", "-");
    }

    @Override
    public void release() {

    }

    /**
     * 根据time_type\time_id删除数据
     *
     * @param table
     */
    public void delete(String table) {

    }

    /**
     * 参考：https://github.com/RedisLabs/spark-redis/blob/master/doc/dataframe.md#dataframe-options
     *
     * @param spark
     * @param sql
     */
    @Override
    public void read(@NotNull SparkSession spark, String sql) {
        String table = Public.getTableFromSelectSQL(sql);
        if ("".equals(table)) {
            return;
        }
        Dataset<Row> df = spark.read()
            .format("org.apache.spark.sql.redis")
            .option("host", this.host)
            .option("port", this.port)
            .option("dbNum", this.dbNO)
            .option("auth", this.auth)
            .option("table", "person")
            .load();
        df.createOrReplaceTempView(table);
        spark.sql(sql);
    }

    /**
     * @param df
     * @param table
     */
    @Override
    public void write(@NotNull Dataset<Row> df, String table) {
        df.write()
            .format("org.apache.spark.sql.redis")
            .option("host", host)
            .option("port", port)
            .option("host", this.host)
            .option("port", this.port)
            .option("dbNum", this.dbNO)
            .option("auth", this.auth)
            .option("table", table)
            .mode(SaveMode.Overwrite)
            .save();
    }

    @Override
    public String getTableColumns(String table) throws Exception {
        return "";
    }

    @Override
    public BiConsumer<String, List<String>> getLoad() throws Exception {
        return null;
    }

    @Override
    public BiConsumer<String, String> getExport() throws Exception {
        return null;
    }

    /**
     * 执行插入的SQL
     *
     * @param sql
     * @throws SQLException
     */
    @Override
    public void exeSQL(String sql) throws SQLException {
        LocalDateTime start = LocalDateTime.now();
        this.logger.info(Public.getMinusSep());
        this.logger.info(sql);

        Public.printDuration(start, LocalDateTime.now());
    }
}

