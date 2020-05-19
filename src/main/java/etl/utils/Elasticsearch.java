package etl.utils;

import com.moandjiezana.toml.Toml;
import com.mongodb.client.MongoClient;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.elasticsearch.spark.sql.api.java.JavaEsSparkSQL;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.time.LocalDateTime;
import java.util.List;
import java.util.function.BiConsumer;

import org.elasticsearch.spark.sql.api.java.JavaEsSparkSQL;


// hive建映射表： https://www.elastic.co/guide/en/elasticsearch/hadoop/current/hive.html

public class Elasticsearch implements DB {
    private final Logger logger = LoggerFactory.getLogger(Rdb.class);
    private final String host;
    private final String port;
    private final String dbType;
    private MongoClient conn;

    Elasticsearch(Toml db) {
        this.host = db.getString("host");
        this.port = db.getString("port", "9200");
        this.dbType = "es";
    }

    @Override
    public void release() {
        this.conn.close();
    }

    /**
     * 根据time_type\time_id删除数据
     *
     * @param table
     */
    public void delete(String table) {

    }

    /**
     * 查询、写入直接在此完成，需要修改extract部分
     * 参考： https://www.elastic.co/guide/en/elasticsearch/hadoop/current/spark.html
     *
     * @param spark
     * @param sql
     * @return
     */
    @Override
    public void read(@NotNull SparkSession spark, String sql) {
        String table = Public.getTableFromSelectSQL(sql);
        if ("".equals(table)) {
            return;
        }
//        Dataset<Row> df = spark.read().format("org.elasticsearch.spark.sql")
        Dataset<Row> df = spark.read().format("es")
            .option("es.nodes", this.host).option("es.port", this.port).option("pushdown", true)
            .load(table + "/_doc"); // { "query" : { "term" : { "user" : "costinl" } } }
        df.createOrReplaceTempView(table);
        spark.sql(sql);
    }

    /**
     * TODO：如何删除重复数据
     * 配置参数： https://www.elastic.co/guide/en/elasticsearch/hadoop/current/configuration.html
     *
     * @param df
     * @param table
     */
    @Override
    public void write(@NotNull Dataset<Row> df, String table) {
//        df.write().mode(SaveMode.Append).format("org.elasticsearch.spark.sql")
//            .option("es.nodes", host).option("es.port", this.port)
//            .save(table + "/_doc");
        JavaEsSparkSQL.saveToEs(df, "spark/docs"); // TODO：host需要在config中指定
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
