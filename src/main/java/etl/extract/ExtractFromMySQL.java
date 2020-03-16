package etl.extract;

import etl.utils.Extract;
import org.apache.spark.sql.SparkSession;

public class ExtractFromMySQL extends Extract {
    public ExtractFromMySQL(SparkSession spark, Integer timeType, String timeID, Integer backDate, String dbID, Integer frequency) {
        super(spark, timeType, timeID, backDate, dbID, frequency);
    }

    public void dpFull() {
        exeSQLFile("dp_d.sql", "insert");
    }
}
