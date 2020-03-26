package etl.export;

import etl.utils.Export;
import org.apache.spark.sql.SparkSession;

import java.sql.SQLException;

/**
 * 导出到MySQl数据库
 */
public class ExportToMySQL extends Export {
    public ExportToMySQL(SparkSession spark, Integer timeType, String timeID, Integer backDate, String dbID, Integer frequency) throws SQLException, ClassNotFoundException {
        super(spark, timeType, timeID, backDate, dbID, frequency);
    }

    /**
     * 导出到数据平台
     *
     * @throws Exception
     */
    public void dpD() throws Exception {
        exeSQLFile("mysql_d.sql", "insert");
    }
}
