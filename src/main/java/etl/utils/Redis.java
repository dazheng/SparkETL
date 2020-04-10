package etl.utils;

import com.moandjiezana.toml.Toml;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Redis {
    private final Logger logger = LoggerFactory.getLogger(RDB.class);
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

    protected String getDbType() {
        return this.dbType;
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
    public void read(@NotNull SparkSession spark, String sql) {
        String table = Public.getTableFromSQL(sql);
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
    protected void write(@NotNull Dataset<Row> df, String table) {
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
}

