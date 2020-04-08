package etl.export;

import etl.utils.Export;
import org.apache.spark.sql.SparkSession;

import java.sql.SQLException;

/**
 * 导出到MySQl数据库
 */
public class ExportToRDB extends Export {
    public ExportToRDB(SparkSession spark, Integer timeType, String timeID, Integer backDate, String dbID, Integer frequency) throws SQLException, ClassNotFoundException {
        super(spark, timeType, timeID, backDate, dbID, frequency);
    }

    /**
     * 导出到数据平台
     *
     * @throws Exception
     */
    public void dpD() throws Exception {
        exeSQLFile("RDB_d.sql", "load"); // exeType: insert 通过jdbc方式插入rdb中；load 将数据导到本地，然后用数据库load方式入库；db: 在rdb中执行SQL
    }
}
