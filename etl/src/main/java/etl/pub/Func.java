package etl.pub;

import com.moandjiezana.toml.Toml;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Properties;

public class Func {
    private final static String MINUS_SEP = "--------------------------------------------"; // sql间分隔符
    private final static String EQUAL_SEP = "======================================================"; // 开始符号
    //    private final static String DEFAULT_COL_DELIMITER = "\u0001";
    private final static String DEFAULT_COL_DELIMITER = ",";
    private static Logger logger = LogManager.getLogger();
    static final Properties PROPERTIES = new Properties(System.getProperties());
    private final static String LineDelimiter = PROPERTIES.getProperty("line.separator");
    private final static String pathDelimiter = PROPERTIES.getProperty("path.separator");
    private static Toml toml = parseParameters();

    static String getMinusSep() {
        return MINUS_SEP;
    }

    public static String getEqualSep() {
        return EQUAL_SEP;
    }

    public static String getDefaultColDelimiter() {
        return DEFAULT_COL_DELIMITER;
    }

    public static String getLineDelimiter() {
        return LineDelimiter;
    }

    public static String getPathDelimiter() {
        return pathDelimiter;
    }

    public static String trim(String s) {
        return s.trim().replaceAll("[\n|\r|\t|\"|" + DEFAULT_COL_DELIMITER + "]", ""); // TODO:修正正则表达式
    }

    static void printDuration(LocalDateTime start, LocalDateTime end) {
        logger.info("time taken %d s", Duration.between(end, start).getSeconds());
    }

    public static String getDataDir() {
        return toml.getTable("base").getString("data_dir");
    }

    public static String getExtractSqlDir() {
        return "extract/";
    }

    public static String getTransformSqlDir() {
        return "transform/";
    }

    public static String getExportSqlDir() {
        return "export/";
    }

//    public static String readSqlFile(String fileName) {
//        File file = new File(fileName);
//        long length = file.length();
//        byte[] content = new byte[(int) length];
//        try {
//            FileInputStream in = new FileInputStream(file);
//            in.read(content);
//            in.close();
//        } catch (IOException e) {
//            logger.fatal(e);
//        }
//        return new String(content, StandardCharsets.UTF_8);
//    }

    public static String readSqlFile(String fileName) {
        StringBuilder sb = null;
        try (
            BufferedReader in =
                new BufferedReader(new InputStreamReader(etl.App.class.getClassLoader().getResourceAsStream(fileName)));) {
            sb = new StringBuilder();
            String line;
            while ((line = in.readLine()) != null) {
                line = line.trim();
                if (line.length() > 0) {
                    sb.append(line);
                    sb.append(getLineDelimiter());
                }
            }
        } catch (IOException e) {
            logger.fatal(e);
        }
        return sb.toString();
    }

    private static Toml parseParameters() {
        InputStreamReader in =
            new InputStreamReader(etl.App.class.getClassLoader().getResourceAsStream("conf.toml"));
        return new Toml().read(in);
//        String file = getProcPath() + "/conf.toml";
//        return new Toml().read(new File(file));
    }

    static Toml getParameters() {
        return toml;
    }

    public String getRootDir() {
        return toml.getTable("base").getString("root_dir");
    }

    public static boolean delDir(File dir) {
        if (dir.isDirectory()) {
            String[] children = dir.list();
            for (int i = 0; i < children.length; i++) {
                boolean success = delDir(new File(dir, children[i]));
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
