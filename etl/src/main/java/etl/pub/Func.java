package etl.pub;

import com.moandjiezana.toml.Toml;
import etl.App;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Objects;
import java.util.Properties;

public class Func {
    private static Logger logger = LoggerFactory.getLogger(Func.class);
    private final static String MINUS_SEP = "--------------------------------------------"; // sql间分隔符
    private final static String EQUAL_SEP = "======================================================"; // 开始符号
    //    private final static String DEFAULT_COL_DELIMITER = "\u0001"; // 数据文件列分隔符
    private final static String DEFAULT_COL_DELIMITER = ",";  // 数据文件列分隔符
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

    public static String getOSLineDelimiter() {
        return LineDelimiter;
    }

    public static String getOSPathDelimiter() {
        return pathDelimiter;
    }

    public static String trim(String s) {
        return s.trim().replaceAll("[\n|\r|\t|\"|" + DEFAULT_COL_DELIMITER + "]", ""); // TODO:修正正则表达式
    }

    static void printDuration(LocalDateTime start, LocalDateTime end) {
        logger.info("time taken {} s", Duration.between(start, end).getSeconds());
    }

    public static String getDataDir() {
        return toml.getTable("base").getString("data_dir");
    }

    public static String readSqlFile(String fileName) {
        StringBuilder sb = null;
        try (
            BufferedReader in =
                new BufferedReader(new InputStreamReader(Objects.requireNonNull(App.class.getClassLoader().getResourceAsStream(fileName))));) {
            sb = new StringBuilder();
            String line;
            while ((line = in.readLine()) != null) {
                line = line.trim();
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

    public String getRootDir() {
        return toml.getTable("base").getString("root_dir");
    }

    public static boolean deleteDir(File dir) {
        if (dir.isDirectory()) {
            String[] children = dir.list();
            assert children != null;
            for (String child : children) {
                boolean success = deleteDir(new File(dir, child));
                if (!success)
                    return false;
            }
        }
        if (dir.delete()) {
            return true;
        } else {
            System.out.println("目录删除失败！！！！！");
        }
        return false;
    }
}
