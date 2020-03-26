package etl.extract;

import etl.utils.Extract;
import org.apache.spark.sql.SparkSession;

import java.sql.SQLException;

/**
 * 从MySQL获取数据
 */
public class ExtractFromMySQL extends Extract {
    public ExtractFromMySQL(SparkSession spark, Integer timeType, String timeID, Integer backDate, String dbID, Integer frequency) throws SQLException, ClassNotFoundException {
        super(spark, timeType, timeID, backDate, dbID, frequency);
    }

    /**
     * 获取全量数据
     *
     * @throws Exception
     */
    public void dpFull() throws Exception {
        exeSQLFile("mysql_d.sql", "insert");
    }
}
