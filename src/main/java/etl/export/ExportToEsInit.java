package etl.export;

import etl.utils.Export;
import org.apache.spark.sql.SparkSession;

public class ExportToEsInit extends Export {
    public ExportToEsInit(SparkSession spark, Integer timeType, String timeID, Integer backDate, String dbID,
                          Integer frequency) throws Exception {
        super(spark, timeType, timeID, backDate, dbID, frequency);
    }

    /**
     * 导出到数据平台
     *
     * @throws Exception
     */
    public void dpInit() throws Exception {
        exeSQLFile("sync.sql", "insert"); // exeType: insert 通过jdbc方式插入Rdb中；load 将数据导到本地，然后用数据库load方式入库；db:
        // 在Rdb中执行SQL
    }
}

