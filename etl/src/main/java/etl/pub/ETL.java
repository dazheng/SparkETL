package etl.pub;

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

    @NotNull
    private Map<String, String> decideByType(@NotNull String line, Integer type) {
        String lineLower = line.toLowerCase();
        Map<String, String> map = new HashMap<String, String>();
        if (type == 1) { // spark平台常规执行
            if (lineLower.contains(" local directory ")) { // 写入目录
                map.put(this.isOrNot, this.is);
                map.put("line", line.split("'")[1]);
                return map;
            }
        } else if (type == 2) { // 导入其他RDBMS数据或者生成临时视图
            if (lineLower.substring(0, 7).equals("insert ")) {
                map.put(this.isOrNot, this.is);
                map.put("line", line);
                return map;
            } else if (lineLower.startsWith("@")) { // 临时视图名称
                map.put(this.isOrNot, this.is);
                map.put("line", line.substring(1).trim());
                return map;
            }
        } else if (type == 3) {  // 导出数据到其他RDBMS
            if (lineLower.startsWith("@")) {
                map.put(this.isOrNot, this.is);
                map.put("line", line.substring(1).trim());
                return map;
            }
        }
        map.put(this.isOrNot, this.not);
        map.put("line", "");
        return map;
    }

    private Map<String, String> parseSQL(String fromSql, Integer type) {
        String isRunable = this.not;
        String insert = "";
        String sql = "";
        String[] s = fromSql.trim().split(Func.getOSLineDelimiter());
        StringBuilder lines = new StringBuilder();
        for (String line : s) {
            String l = line.trim().toLowerCase();
            if (!l.substring(0, 2).equals("--") && !l.substring(0, 2).equals("//")) {
                isRunable = this.is;
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
            lines.append(line).append(Func.getOSLineDelimiter());
        }
        sql = lines.toString();
        Map<String, String> map = new HashMap<String, String>();
        map.put(isOrNot, isRunable);
        map.put("insert", insert);
        map.put("sql", sql.trim());
        return map;
    }

    private String replaceSQLParameter(String sql) {
        Map<String, String> paras = getTimeParameters();
        for (Map.Entry<String, String> entry : paras.entrySet()) {
            sql = sql.replace("${" + entry.getKey() + "}", entry.getValue());
        }
        return sql;
    }

    void exeSQLs(String sqlString, BiConsumer<String, String> func, Integer type) {
        if (sqlString == null || func == null || sqlString.trim().isEmpty() ) {
            return;
        }
        String[] s = sqlString.split(";");
        for (String sql : s) {
            sql = sql.trim();
            if (sql.length() < 7) {
                return;
            }
            sql = replaceSQLParameter(sql);
            Map<String, String> rs = parseSQL(sql, type);
            String f = rs.get(this.isOrNot);
            String insert = rs.get("insert");
            sql = rs.get("sql");
            if (f.equals(this.is)) {
                func.accept(insert, sql);
            }
        }
    }

    Dataset<Row> exeSQL(String sql) {
        LocalDateTime start = LocalDateTime.now();
        logger.info(Func.getMinusSep());
        logger.info(sql);
        Dataset<Row> df = spark.sql(sql);
        Func.printDuration(start, LocalDateTime.now());
        return df;
    }

    void toLocalDir(Dataset<Row> df, String localDir) {
        FileSystem fileSystem = null;  //操作Hdfs核心类
        Configuration configuration = null;  //配置类
        Toml toml = Func.getParameters();
        String HDFS_PATH = toml.getTable("base").getString("hdfs_path");
        try {
            String hdfsDir = "/tmp/" + localDir.substring(localDir.indexOf("/"));
            df.coalesce(8).write().mode("overwrite").option("sep", Func.getColumnDelimiter()).csv(hdfsDir);
            Files.createDirectories(Paths.get(localDir));
            Func.deleteDir(new File(localDir));
            configuration = new Configuration();
            configuration.set("fs.defaultFS", HDFS_PATH);
            fileSystem = FileSystem.get(configuration);
            fileSystem.copyToLocalFile(new Path(hdfsDir), new Path(localDir));
        } catch (Exception e) {
            logger.error(e.toString(), e);
        } finally {
            assert configuration != null;
            configuration.clear();
            assert fileSystem != null;
            try {
                fileSystem.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    void sqlSpecialView(String table, String sql) {
        Dataset<Row> df = exeSQL(sql);
        df.createOrReplaceTempView("v_" + table);
    }
}
