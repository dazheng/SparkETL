package etl.utils;

import com.moandjiezana.toml.Toml;
import com.mongodb.client.MongoClient;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.elasticsearch.spark.sql.api.java.JavaEsSparkSQL;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Elasticsearch {
    private final Logger logger = LoggerFactory.getLogger(RDB.class);
    private final String host;
    private final String port;
    private final String dbType;
    private MongoClient conn;

    Elasticsearch(Toml db) {
        this.host = db.getString("host");
        this.port = db.getString("port", "9200");
        this.dbType = "es";
    }

    protected String getDbType() {
        return this.dbType;
    }

    protected void release() {
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
    public void read(@NotNull SparkSession spark, String sql) {
        String table = Public.getTableFromSQL(sql);
        Dataset<Row> df = spark.read().format("org.elasticsearch.spark.sql").option("es.nodes", this.host)
            .option("es.port", this.port).option("pushdown", true).load(table + "/_doc"); // { "query" : { "term" : { "user" : "costinl" } } }
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
    public void write(@NotNull Dataset<Row> df, String table) {
//        df.write().format("org.elasticsearch.spark.sql").option("es.nodes", host).option("es.port", this.port).mode("append").save(type + "/" + table);
        JavaEsSparkSQL.saveToEs(df, table + "/_doc");
    }


}
