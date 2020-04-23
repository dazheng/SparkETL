package etl.extract;

import etl.utils.Extract;
import org.apache.spark.sql.SparkSession;

/**
 * 从MySQL获取数据
 */
public class ExtractFromDBDaily extends Extract {
    public ExtractFromDBDaily(SparkSession spark, Integer timeType, String timeID, Integer backDate, String dbID, Integer frequency) throws Exception {
        super(spark, timeType, timeID, backDate, dbID, frequency);
    }

    /**
     * 获取全量数据
     *
     * @throws Exception
     */
    public void dpFull() throws Exception {
        exeSQLFile("extract_d.sql", "load");  // exeType: insert jdbc方式导入；load 通过Rdb命令或者jdbc方式生成文件，然后load到hive表中
    }
}
