package etl.extract;

import etl.pub.Extract;
import org.apache.spark.sql.SparkSession;

public class ExtMySQL extends Extract {
    public ExtMySQL(SparkSession spark, Integer timeType, String timeID, Integer backDate, String dbID, Integer frequency) {
        super(spark, timeType, timeID, backDate, dbID, frequency);
    }

    public void dpFull() {
        exeSQLFile("dp_d.sql", "insert");
    }
}
