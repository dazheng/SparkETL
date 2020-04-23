package etl.utils;

import com.moandjiezana.toml.Toml;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;
import java.util.function.BiConsumer;


/**
 * ETL处理的主程序
 */
public class ETL extends Time {
    private final Logger logger = LoggerFactory.getLogger(Time.class);
    private final SparkSession spark;
    private final String isOrNot = "isOrNot";
    private final String is = "is";
    private final String not = "not";


    ETL(SparkSession spark, Integer timeType, String timeID, Integer backDate, Integer frequency) {
        super(timeType, timeID, backDate, frequency);
        this.spark = spark;
    }

    /**
     * 根据类型不同，对输入的单独一行进行不同的处理
     *
     * @param line 要解析的行
     * @param type 解析类型
     * @return 解析后的sql片段
     */
    @NotNull
    private Map<String, String> decideByType(@NotNull String line, Integer type) {
        String lineLower = line.toLowerCase();
        Map<String, String> map = new HashMap<String, String>();
        switch (type) {
            case 1:  // spark平台常规执行
                if (lineLower.contains(" local directory ")) { // 写入目录
                    map.put(this.isOrNot, this.is);
                    map.put("line", line.split("'")[1]);
                    return map;
                }
                break;
            case 2: // 导入其他RdbMS数据或者生成临时视图
                if (lineLower.substring(0, 7).equals("insert ")) {
                    map.put(this.isOrNot, this.is);
                    map.put("line", line);
                    return map;
                } else if (lineLower.startsWith("@")) { // 临时视图名称
                    map.put(this.isOrNot, this.is);
                    map.put("line", line.substring(1).trim());
                    return map;
                }
            case 3:  // 导出数据到其他RdbMS
                if (lineLower.startsWith("@")) {
                    map.put(this.isOrNot, this.is);
                    map.put("line", line.substring(1).trim());
                    return map;
                }
                break;
        }
        map.put(this.isOrNot, this.not);
        map.put("line", "");
        return map;
    }

    /**
     * 根据不同类型，进行不同的解析
     *
     * @param fromSql 要解析的SQL
     * @param type    类型
     * @return 解析后的SQL
     */
    private Map<String, String> parseSQL(String fromSql, Integer type) {
        String isRunable = this.not;
        String insert = "";
        String sql = "";
        String[] s = fromSql.trim().split(Public.getOSLineDelimiter());
        StringBuilder lines = new StringBuilder();
        for (String line : s) {
            String l = line.trim().toLowerCase();
            if (!l.substring(0, 2).equals("--") && !l.substring(0, 2).equals("//")) {
                isRunable = this.is;
            }
            l = l.split("--")[0];
            if (l.trim().length() < 2) {
                continue;
            }
            if (isRunable.equals(this.is)) {
                Map<String, String> rs = decideByType(l, type);
                String isInsert = rs.get(this.isOrNot);
                sql = rs.get("line");
                if (isInsert.equals(this.is)) {
                    insert = sql;
                    continue;
                }
            }
            lines.append(line).append(Public.getOSLineDelimiter());
        }
        sql = lines.toString();
        Map<String, String> map = new HashMap<String, String>();
        map.put(isOrNot, isRunable);
        map.put("insert", insert);
        map.put("sql", sql.trim());
        return map;
    }

    /**
     * 建SQL语句中的参数用具体的值替换掉
     *
     * @param sql SQL语句
     * @return 替换后的SQL
     */
    private String replaceSQLParameters(String sql) {
        Map<String, String> paras = getTimeParameters();
        for (Map.Entry<String, String> entry : paras.entrySet()) {
            sql = sql.replace("${" + entry.getKey() + "}", entry.getValue());
        }
        return sql;
    }

    /**
     * 执行N个SQL语句
     *
     * @param sqlString SQL内容端
     * @param func      执行单个SQL的方法
     * @param type      类型
     * @throws Exception
     */
    void exeSQLs(String sqlString, BiConsumer<String, String> func, Integer type) throws Exception {
        if (sqlString == null || func == null || sqlString.trim().isEmpty()) {
            return;
        }
        String[] s = sqlString.split(";");
        for (String sql : s) {
            sql = sql.trim();
            if (sql.length() < 7) {
                return;
            }
            sql = replaceSQLParameters(sql);
            Map<String, String> rs = parseSQL(sql, type);
            String f = rs.get(this.isOrNot);
            String insert = rs.get("insert");
            sql = rs.get("sql");
            if (f.equals(this.is)) {
                func.accept(insert, sql);
            }
        }
    }


    /**
     * 执行SQL
     *
     * @param sql
     * @return
     */
    Dataset<Row> exeSQL(String sql) {
        LocalDateTime start = LocalDateTime.now();
        this.logger.info(Public.getMinusSep());
        this.logger.info(sql);
        Dataset<Row> df = spark.sql(sql);
        Public.printDuration(start, LocalDateTime.now());
        return df;
    }

    /**
     * 将Dataset的结果生成hdfs文件，然后再同步到本地
     *
     * @param df       Dataset
     * @param localDir 本地目录
     * @throws IOException
     */
    void toLocalDirectory(Dataset<Row> df, String localDir) throws IOException {
        FileSystem fileSystem = null;  //操作Hdfs核心类
        Configuration configuration = null;  //配置类
        Toml toml = Public.getParameters();
        String HDFS_PATH = toml.getTable("base").getString("hdfs_path", "hdfs://master");
        try {
            String hdfsDir = "/tmp/" + localDir.substring(localDir.indexOf("/"));
            df.coalesce(8).write().mode("overwrite").option("sep", Public.getColumnDelimiter()).csv(hdfsDir);
            Files.createDirectories(Paths.get(localDir));
            Public.deleteDirectory(new File(localDir));
            configuration = new Configuration();
            configuration.set("fs.defaultFS", HDFS_PATH);
            fileSystem = FileSystem.get(configuration);
            fileSystem.copyToLocalFile(new Path(hdfsDir), new Path(localDir));
        } finally {
            if (configuration != null) {
                configuration.clear();
            }
            if (fileSystem != null) {
                fileSystem.close();
            }
        }
    }

    /**
     * sql语句生成指定的临时视图
     *
     * @param table 表名
     * @param sql   SQL语句
     */
    void sqlToSpecialView(String table, String sql) {
        Dataset<Row> df = exeSQL(sql);
        df.createOrReplaceTempView("v_" + table);
    }

    /**
     * 将文件load到hive中
     *
     * @param insertSQL SQL语句
     * @param file      文件
     */
    void hiveLoad(String insertSQL, String file) {
        String sql = String.format("load data local inpath '%s' %s ", file, getHiveLoad(insertSQL));
        exeSQL(sql);
    }

    /**
     * 根据insert SQL语句获取load语句
     *
     * @param insertSQL insert语句
     * @return load语句
     */
    private String getHiveLoad(String insertSQL) {
        return insertSQL.replace("insert overwrite", "overwrite into")
            .replace("insert into", "into");
    }
}
