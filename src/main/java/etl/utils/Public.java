package etl.utils;

import com.moandjiezana.toml.Toml;
import etl.App;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Objects;
import java.util.Properties;

public class Public {
    private static Logger logger = LoggerFactory.getLogger(Public.class);
    private final static String MINUS_SEP = "--------------------------------------------"; // sql间分隔符
    private final static String EQUAL_SEP = "======================================================"; // 开始符号
    private final static String DEFAULT_COL_DELIMITER = "\u0001"; // 数据文件列分隔符
    //    private final static String DEFAULT_COL_DELIMITER = ",";  // 数据文件列分隔符
    static final Properties PROPERTIES = new Properties(System.getProperties());
    private final static String LineDelimiter = PROPERTIES.getProperty("line.separator"); // 操作系统换行符
    private final static String pathDelimiter = PROPERTIES.getProperty("path.separator"); // 操作系统路径分隔符

    private static Toml toml = parseParameters();

    static String getMinusSep() {
        return MINUS_SEP;
    }

    public static String getEqualSep() {
        return EQUAL_SEP;
    }

    public static String getColumnDelimiter() {
        return DEFAULT_COL_DELIMITER;
    }

    public static String getColumnDelimiterRDB() {
        if(getColumnDelimiter().equals("\u0001")) {
            return "x'01'";
        }
        return getColumnDelimiter();
    }
    public static String getOSLineDelimiter() {
        return LineDelimiter;
    }

    public static String getOSPathDelimiter() {
//        return pathDelimiter;
        return "/";
    }

    public static String trim(String s) {
        return s.trim().replaceAll("[\n|\r|\t|\"|" + DEFAULT_COL_DELIMITER + "]", ""); // TODO:修正正则表达式
    }

    static void printDuration(LocalDateTime start, LocalDateTime end) {
        logger.info("time taken {} s", Duration.between(start, end).getSeconds());
    }

    public static String getDataDirectory() {
        return toml.getTable("base").getString("data_dir");
    }

    public static String getLogDirectory() {
        return toml.getTable("base").getString("log_dir");
    }

    public static String getConfDirectory() {
        return toml.getTable("base").getString("conf_dir");
    }

    public static String getTableDataDirectory(String table, String timeType) {
        return getDataDirectory() + table + "/" + timeType + "/";
    }

    // TODO: 一次读取整个文件
    public static String readSqlFile(String fileName) {
        StringBuilder sb = null;
        try (
            BufferedReader in =
                new BufferedReader(new InputStreamReader(Objects.requireNonNull(App.class.getClassLoader().getResourceAsStream(fileName))));) {
            sb = new StringBuilder();
            String line;
            while ((line = in.readLine()) != null) {
                line = line.trim().replace("\r\n", getOSLineDelimiter());
                if (line.length() > 0) {
                    sb.append(line);
                    sb.append(getOSLineDelimiter());
                }
            }
        } catch (IOException e) {
            logger.error(e.toString(), e);
        }
        assert sb != null;
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


    public static boolean deleteDirectory(File dir) {
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

    public static void exeCmd(String cmd) {
        Process p = null;
        InputStream ins = null;
        try {
            p = Runtime.getRuntime().exec(cmd);
            ins = p.getInputStream();
            Charset charset = StandardCharsets.UTF_8;
            BufferedReader reader = new BufferedReader(new InputStreamReader(ins, charset));
            String line = null;
            while ((line = reader.readLine()) != null) {
                String msg = new String(line.getBytes(), "GBK");
                logger.info(msg);
            }
            int exitCode = p.waitFor();
            if (exitCode == 0) {
                logger.info("{} succeed", cmd);
            } else {
                logger.error("{} failed", cmd);
            }
        } catch (IOException | InterruptedException e) {
            logger.error(e.toString(), e);
        }
    }
}
