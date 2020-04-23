package etl.utils;

import com.moandjiezana.toml.Toml;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.spark.MongoSpark;
import com.mongodb.spark.config.ReadConfig;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;

public class MongoDB implements DB {
    private final Logger logger = LoggerFactory.getLogger(Rdb.class);
    private final String url;
    private final String dbType;
    private MongoClient conn;


    MongoDB(Toml db) {
        assert db != null;
        this.url = db.getString("url");
        this.dbType = Public.DB_MONGODB;
        this.conn = connection();
    }

    @Override
    public void release() {
        this.conn.close();
    }

    private MongoClient connection() {
        MongoClient mongoClient = null;
        try {
            mongoClient = MongoClients.create(this.url);
        } catch (Exception e) {
            logger.error(e.toString(), e);
        }
        return mongoClient;
    }

    protected MongoClient getConnection() {
        return this.conn;
    }

    /**
     * 根据time_type\time_id删除数据
     *
     * @param table
     */
    public void delete(String table) {

    }

    /**
     * 参考：https://docs.mongodb.com/spark-connector/master/configuration/
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
        JavaSparkContext jsc = createJavaSparkContext(this.url);
        Map<String, String> readOverrides = new HashMap<String, String>();
        readOverrides.put("collection", table);
        readOverrides.put("batchSize", String.valueOf(Public.getJdbcFetchSize()));
        ReadConfig readConfig = ReadConfig.create(jsc).withOptions(readOverrides);
        Dataset<Row> explicitDF = MongoSpark.load(jsc, readConfig).toDF();
        explicitDF.createOrReplaceTempView(table);
        spark.sql(sql);
    }

    /**
     * TODO：如何删除重复数据？
     *
     * @param df
     * @param table
     */
    @Override
    public void write(@NotNull Dataset<Row> df, String table) {
        MongoSpark.write(df).mode(SaveMode.Append).option("collection", table).option("maxBatchSize", Public.getJdbcBatchSize()).save();
    }

    private static JavaSparkContext createJavaSparkContext(String uri) {
        SparkConf conf = new SparkConf()
            .set("spark.mongodb.input.uri", uri)
            .set("spark.mongodb.output.uri", uri);
        return new JavaSparkContext(conf);
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

