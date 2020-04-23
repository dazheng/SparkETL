package etl.utils;

import com.moandjiezana.toml.Toml;
import com.mongodb.client.MongoClient;
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

public class Kudu implements DB {
    private final Logger logger = LoggerFactory.getLogger(Rdb.class);
    private final String host;
    private final String dbType;
    private MongoClient conn;


    Kudu(Toml db) {
        assert db != null;
        this.host = db.getString("host");
        this.dbType = Public.DB_MONGODB;
    }

    @Override
    public void release() {
        this.conn.close();
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
        Dataset<Row> df = spark.read().format("org.apache.kudu.spark.kudu")
            .option("kudu.master", this.host)
            .option("kudu.table", table)
            .load();
        df.createOrReplaceTempView(table);
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
        df.write()
            .mode(SaveMode.Append) // 只支持Append模式，键相同的会自动覆盖
            .format("org.apache.kudu.spark.kudu")
            .option("kudu.master", this.host)
            .option("kudu.table", table)
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
