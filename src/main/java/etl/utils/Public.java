package etl.utils;

import com.moandjiezana.toml.Toml;
import etl.App;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.*;
import java.util.function.BiConsumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 公用的功能
 */
public class Public {
    private static Logger logger = LoggerFactory.getLogger(Public.class);
    private final static String MINUS_SEP = "--------------------------------------------"; // sql间分隔符
    private final static String EQUAL_SEP = "======================================================"; // 开始符号
    private final static String DEFAULT_COL_DELIMITER = "\u0001"; // 数据文件列分隔符
    //    private final static String DEFAULT_COL_DELIMITER = ",";  // 数据文件列分隔符
    static final Properties PROPERTIES = new Properties(System.getProperties());
    private final static String LineDelimiter = PROPERTIES.getProperty("line.separator"); // 操作系统换行符
    private final static String FileDelimiter = PROPERTIES.getProperty("file.separator"); // 操作系统路径分隔符
    final static String DB_Rdb = "rdb";
    final static String DB_ORACLE = "oracle";
    final static String DB_MYSQL = "mysql";
    final static String DB_SQLSERVER = "sqlserver";
    final static String DB_POSTGRESQL = "postgresql";
    final static String DB_DB2 = "db2";
    final static String DB_MONGODB = "mongo";
    final static String DB_ELASTICSEARCH = "es";
    final static String DB_REDIS = "redis";
    final static String DB_KUDU = "kudu";
    final static String[] TOML_DB_TABLE = {DB_Rdb, DB_MONGODB, DB_ELASTICSEARCH, DB_REDIS, DB_KUDU};
    private static Toml toml = parseParameters(); // 获取解析后的toml配置文件

    static String getMinusSep() {
        return MINUS_SEP;
    }

    public static String getEqualSep() {
        return EQUAL_SEP;
    }

    public static String getColumnDelimiter() {
        return DEFAULT_COL_DELIMITER;
    }

    public static String getColumnDelimiterRdb() {
        if (getColumnDelimiter().equals("\u0001")) {
            return "x'01'";
        }
        return getColumnDelimiter();
    }

    public static String getOSLineDelimiter() {
        return LineDelimiter;
    }

    public static String getOSFileDelimiter() { // 目前只支持Linux系统
        return FileDelimiter;
    }

    public static void printDuration(LocalDateTime start, LocalDateTime end) {
        logger.info("time taken {} s", Duration.between(start, end).getSeconds());
    }

    protected static String getDataDirectory() {
        return toml.getTable("base").getString("data_dir");
    }

    protected static String getLogDirectory() {
        return toml.getTable("base").getString("log_dir");
    }

    protected static String getConfDirectory() {
        return toml.getTable("base").getString("conf_dir");
    }

    protected static String getTableDataDirectory(String table, String timeType) {
        return getDataDirectory() + table + "/" + timeType + "/";
    }


    /**
     * 读取sql文件
     *
     * @param fileName 文件名
     * @return 文件内容
     * @throws IOException
     */
    public static String readSqlFile(String fileName) throws IOException {
        StringBuilder sb = null;
        BufferedReader in =
            new BufferedReader(new InputStreamReader(Objects.requireNonNull(App.class.getClassLoader().getResourceAsStream(fileName))));
        sb = new StringBuilder();
        String line;
        Pattern p = Pattern.compile("\\s+");
        while ((line = in.readLine()) != null) {
            line = line.trim().replace("\r\n", getOSLineDelimiter());
            Matcher m = p.matcher(line);
            line = m.replaceAll(" ");
            if (line.length() > 0) {
                sb.append(line);
                sb.append(getOSLineDelimiter());
            }
        }
        in.close();
        return sb.toString();
    }

    private static Toml parseParameters() {
        InputStreamReader in =
            new InputStreamReader(Objects.requireNonNull(App.class.getClassLoader().getResourceAsStream("conf.toml")));
        return new Toml().read(in);
    }

    static Toml getParameters() {
        return toml;
    }

    /**
     * jdbc 获取数据时每批次记录条数
     *
     * @return 记录条数
     */
    static long getJdbcFetchSize() {
        return getParameters().getTable("base").getLong("jdbc_fetch_size", (long) 10000);
    }

    /**
     * jdbc 写入数据时每批次记录条数
     *
     * @return 记录条数
     */
    static long getJdbcBatchSize() {
        return getParameters().getTable("base").getLong("jdbc_batch_size", (long) 10000);
    }

    /**
     * 获取配置文件中dbID对应的toml信息
     *
     * @param dbID db标识符
     * @return db对应的toml信息
     */
    static Map<String, Object> getDB(String dbID) {
        Toml toml = getParameters();
        Map<String, Object> map = new HashMap<>();
        for (String table : TOML_DB_TABLE) {
            List<Toml> dbs = toml.getTables(table);
            for (Toml db : dbs) {
                if (db.getString("id").equals(dbID)) {
                    map.put("type", table);
                    map.put("db", db);
                    return map;
                }
            }
        }
        return null;
    }

    /**
     * 递归删除目录先的所有文件及目录
     *
     * @param dir 目录
     * @return 是否成功
     */
    static boolean deleteDirectory(File dir) {
        if (dir.isDirectory()) {
            String[] children = dir.list();
            assert children != null;
            for (String child : children) {
                boolean success = deleteDirectory(new File(dir, child));
                if (!success)
                    return false;
            }
        }
        if (dir.delete()) {
            return true;
        } else {
            logger.warn("delete {} failed", dir);
        }
        return false;
    }

    /**
     * 从单表查询语句中获得表名
     *
     * @param sql 查询SQL
     * @return 表名
     */
    static String getTableFromSelectSQL(String sql) {
        sql = sql.toLowerCase();
        int index = sql.indexOf("from ");
        sql = sql.substring(index + 5);
        Pattern p = Pattern.compile("\\s+");
        Matcher m = p.matcher(sql);
        if (!m.find()) {
            return sql;
        }
        index = m.start();
        return sql.substring(0, index);
    }

    /**
     * 从insert overwrite[into] table的SQL中获取目标表名
     *
     * @param insertSQL 插入SQL
     * @return 目标表名
     */
    static String getTableFromInsertSQL(String insertSQL) {
        String[] sqls = insertSQL.split(" ");
        for (int i = 0; i < sqls.length; i++) {
            if ("table".equals(sqls[i])) {
                if (!sqls[i + 1].equals(" ")) {
                    return sqls[i + 1];
                }
            }
        }
        return null;
    }

    /**
     * 执行命令行
     *
     * @param cmd 命令
     * @throws InterruptedException
     * @throws IOException
     */
    static void exeCmd(String cmd) throws InterruptedException, IOException {
        logger.info(Public.getMinusSep());
        logger.info(cmd);
        Process p = Runtime.getRuntime().exec(cmd);
        InputStream ins = p.getInputStream();
        BufferedReader reader = new BufferedReader(new InputStreamReader(ins, StandardCharsets.UTF_8));
        String line = null;
        while ((line = reader.readLine()) != null) {
            String msg = new String(line.getBytes(), StandardCharsets.UTF_8);
            logger.info(msg);
        }
        int exitCode = p.waitFor();
        if (exitCode == 0) {
            logger.info("{} succeed", cmd);
        } else {
            logger.error("{} failed", cmd);
        }
    }

    /**
     * 处理lamda的异常
     *
     * @param <T>
     * @param <U>
     * @param <E>
     */
    @FunctionalInterface
    interface BiConsumerWithExceptions<T, U, E extends Exception> {
        void accept(T t, U u) throws E;
    }

    static <T, U, E extends Exception> BiConsumer<T, U> rethrowBiConsumer(BiConsumerWithExceptions<T, U, E> biConsumer) throws E {
        return (t, u) -> {
            try {
                biConsumer.accept(t, u);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        };
    }


    public static class JdbcUrlSplitter {
        public String driverName, host, port, database, params;

        public JdbcUrlSplitter(String jdbcUrl) {
            int pos, pos1, pos2;
            String connUri;

            if (jdbcUrl == null || !jdbcUrl.startsWith("jdbc:")
                || (pos1 = jdbcUrl.indexOf(':', 5)) == -1)
                throw new IllegalArgumentException("Invalid JDBC url.");

            driverName = jdbcUrl.substring(5, pos1);
            if ((pos2 = jdbcUrl.indexOf(';', pos1)) == -1) {
                connUri = jdbcUrl.substring(pos1 + 1);
            } else {
                connUri = jdbcUrl.substring(pos1 + 1, pos2);
                params = jdbcUrl.substring(pos2 + 1);
            }

            if (connUri.startsWith("//") || connUri.startsWith("thin:@")) {
                if (connUri.startsWith("thin:@")) {
                    connUri = connUri.substring(6);
                }
                if (driverName.equals("sqlserver")) {
                    String[] parmsArray = params.split(";");
                    for (String p : parmsArray) {
                        String[] ps = p.split("=");
                        if (ps.length < 2) {
                            throw new IllegalArgumentException("Invalid JDBC url.");
                        }
                        if (ps[0].toLowerCase().equals("databasename")) {
                            database = ps[1];
                            break;
                        }
                    }
                    String[] hostPort = connUri.split(":");
                    host = hostPort[0].substring(2);
                    if (hostPort.length >= 2) {
                        port = hostPort[1];
                    }
                } else if ((pos = connUri.indexOf('/', 2)) != -1) {
                    host = connUri.substring(2, pos);
                    database = connUri.substring(pos + 1);
                    int index = database.indexOf("?");
                    if (index != -1) {
                        database = database.substring(0, index);
                    }

                    if ((pos = host.indexOf(':')) != -1) {
                        port = host.substring(pos + 1);
                        host = host.substring(0, pos);
                    }
                }
            } else {
                database = connUri.substring(0, connUri.indexOf("?"));
            }
        }
    }
}


