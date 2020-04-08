package etl.extract;

import etl.utils.Extract;
import org.apache.spark.sql.SparkSession;

import java.sql.SQLException;

/**
 * 从MySQL获取数据
 */
public class ExtractFromRDB extends Extract {
    public ExtractFromRDB(SparkSession spark, Integer timeType, String timeID, Integer backDate, String dbID, Integer frequency) throws SQLException, ClassNotFoundException {
        super(spark, timeType, timeID, backDate, dbID, frequency);
    }

    /**
     * 获取全量数据
     *
     * @throws Exception
     */
    public void dpFull() throws Exception {
        exeSQLFile("RDB_d.sql", "load");  // exeType: insert jdbc方式导入；load 通过rdb命令或者jdbc方式生成文件，然后load到hive表中
    }
}
