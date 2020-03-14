package etl.export;

import etl.pub.Export;
import org.apache.spark.sql.SparkSession;

public class ExpMySQL extends Export {
    public ExpMySQL(SparkSession spark, Integer timeType, String timeID, Integer backDate, String dbID, Integer frequency) {
        super(spark, timeType, timeID, backDate, dbID, frequency);
    }

    public void dpD() {
        exeSQLFile("dp_d.sql", "insert");
    }
}
