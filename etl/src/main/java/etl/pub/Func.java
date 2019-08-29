package etl.pub;

import com.moandjiezana.toml.Toml;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.LocalDateTime;

public class Func {

    private static Logger logger = LogManager.getLogger();
    private final static String MINUS_SEP = "--------------------------------------------"; // sql间分隔符
    private final static String EQUAL_SEP = "======================================================"; // 开始符号
    private final static String DEFAULT_COL_DELIMITER = "\u0001";
    private final static String ROOT_DIR = "c:/dp/";
    private final static String DATA_DIR = ROOT_DIR + "data/";
    private final static String SQL_FILE_DIR = getProcPath() + "/resource/";

    static String getMinusSep() {
        return MINUS_SEP;
    }

    public static String getEqualSep() {
        return EQUAL_SEP;
    }

    public static String getDefaultColDelimiter() {
        return DEFAULT_COL_DELIMITER;
    }

    public static String getRootDir() {
        return ROOT_DIR;
    }

    public static String trim(String s) {
        return s.trim().replaceAll("[\n|\r|\t|\"|" + DEFAULT_COL_DELIMITER + "]", ""); // TODO:修正正则表达式
    }

    static void printDuration(LocalDateTime start, LocalDateTime end) {
        logger.info("time taken %d s", Duration.between(end, start).getSeconds());
    }

    static String getDataDir() {
        return DATA_DIR;
    }

    public static String getExtractSqlDir() {
        return SQL_FILE_DIR + "extract/";
    }

    public static String getTransformSqlDir() {
        return SQL_FILE_DIR + "etl/transform/";
    }

    public static String getExportSqlDir() {
        return SQL_FILE_DIR + "export/";
    }

    public static String readSqlFile(String fileName) {
        File file = new File(fileName);
        long length = file.length();
        byte[] content = new byte[(int) length];
        try {
            FileInputStream in = new FileInputStream(file);
            in.read(content);
            in.close();
        } catch (IOException e) {
            logger.error(e);
        }
        return new String(content, StandardCharsets.UTF_8);
    }

    public static String getProcPath() {
        String filePath = "";
        try {
            File files = new File("");
            filePath = files.getCanonicalPath();
            logger.debug("proc path is: %s", filePath);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return filePath;
    }

    public static Toml getParameters() {
        String file = getProcPath() + "/resource/conf.toml";
        return new Toml().read(new File(file));
    }
}
