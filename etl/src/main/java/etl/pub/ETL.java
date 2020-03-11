package etl.pub;

import com.moandjiezana.toml.Toml;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;
import java.util.function.BiConsumer;

import static etl.pub.Func.delDir;
import static etl.pub.Func.getLineDelimiter;


class ETL extends Time {
    private final Logger logger = LogManager.getLogger();
    private final SparkSession session;
    private final String isOrNot = "isOrNot";
    private final String is = "is";
    private final String not = "not";


    ETL(SparkSession session, Integer timeType, String timeID, Integer backDate, Integer frequency) {
        super(timeType, timeID, backDate, frequency);
        this.session = session;
    }

    private Map<String, String> decideByType(String line, Integer type) {
        String lineLower = line.toLowerCase();
        Map<String, String> map = new HashMap<String, String>();
        if (type == 1 || type == 4) {
            if (lineLower.contains(" local directory ")) { // 写入目录
                map.put(this.isOrNot, this.is);
                map.put("line", line.split("'")[1]);
                return map;
            }
        } else if (type == 2) {// 导入其他RDBMS数据
            if (lineLower.substring(0, 7).equals("insert ")) {
                map.put(this.isOrNot, this.is);
                map.put("line", line);
                return map;
            } else if (lineLower.startsWith("@")) {
                map.put(this.isOrNot, this.is);
                map.put("line", line.substring(1).trim());
                return map;
            }
        } else if (type == 3) { // 导出数据到其他RDBMS
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

    private Map<String, String> parseSql(String fromSql, Integer type) {
        String f = this.not;
        String dir = "";
        String sql = "";
        String[] s = fromSql.trim().split(getLineDelimiter());
        StringBuilder lines = new StringBuilder();
        for (String line : s) {
            String l = line.trim().toLowerCase();
            if (!l.substring(0, 2).equals("--") && !l.substring(0, 2).equals("//")) {
                f = this.is;
            }
            if (f.equals(this.is)) {
                Map<String, String> rs = decideByType(l, type);
                String f2 = rs.get(this.isOrNot);
                sql = rs.get("line");
                if (f2.equals(this.is)) {
                    dir = sql;
                    continue;
                }
            }
            lines.append(line).append(getLineDelimiter());
        }
        sql = lines.toString();
        Map<String, String> map = new HashMap<String, String>();
        map.put(isOrNot, f);
        map.put("dir", dir);
        map.put("sql", sql.trim());
        return map;
    }

    private String replaceSqlParameter(String sql) {
        Map<String, String> paras = getTimeParameters();
        for (Map.Entry<String, String> entry : paras.entrySet()) {
            sql = sql.replace("${" + entry.getKey() + "}", entry.getValue());
        }
        return sql;
    }

    void exeSqls(String sqlString, BiConsumer<String, String> func, Integer type) {
        if (sqlString == null || func == null) {
            return;
        }
        String[] s = sqlString.split(";");
        for (String sql : s) {
            sql = sql.trim();
            if (sql.length() < 7) {
                return;
            }
            sql = replaceSqlParameter(sql);
            Map<String, String> rs = parseSql(sql, type);
            String f = rs.get(this.isOrNot);
            String dir = rs.get("dir");
            sql = rs.get("sql");
            if (f.equals(this.is)) {
                func.accept(dir, sql);
            }
        }
    }

    Dataset<Row> exeSql(String sql) {
        LocalDateTime start = LocalDateTime.now();
        logger.info(Func.getMinusSep());
        logger.info(sql);
        Dataset<Row> df = session.sql(sql);
        logger.info("time taken: %d s", Duration.between(LocalDateTime.now(), start).getSeconds());
        return df;
    }

    void toLocalDir(Dataset df, String localDir) {
        FileSystem fileSystem = null;  //操作Hdfs核心类
        Configuration configuration = null;  //配置类
        Toml toml = Func.getParameters();
        String HDFS_PATH = toml.getTable("base").getString("hdfs_path");
        try {
            String hdfsDir = "/tmp/" + localDir.substring(localDir.indexOf("/"));
            df.coalesce(8).write().mode("overwrite").option("sep", Func.getDefaultColDelimiter()).csv(hdfsDir);
//            localDir = localDir.substring(0, localDir.length() - 2);
            Files.createDirectories(Paths.get(localDir));
//            Files.deleteIfExists(Paths.get(localDir + "/*"));
            delDir(new File(localDir));
            configuration = new Configuration();
            configuration.set("fs.defaultFS", HDFS_PATH);
            fileSystem = FileSystem.get(configuration);
            fileSystem.copyToLocalFile(new Path(hdfsDir), new Path(localDir));
//            String shStr = String.format("hadoop fs -get %s* %s", hdfsDir, localDir);
//            Process p = Runtime.getRuntime().exec(new String[]{"/bin/sh","-c",shStr}); // TODO:切换至java方式
//            InputStreamReader ir = new InputStreamReader(p.getInputStream());
//            LineNumberReader input = new LineNumberReader(ir);
//            String line;
//            p.waitFor();
//            List<String> strList = new ArrayList<String>();
//            while ((line = input.readLine()) != null){
//                strList.add(line);
//            }
//            System.out.println(strList.toString());
        } catch (Exception e) {
            logger.fatal(e);
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
        Dataset<Row> df = exeSql(sql);
        df.createOrReplaceTempView("v_" + table);
    }
}
