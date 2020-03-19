package etl.export;

import etl.utils.Export;
import org.apache.spark.sql.SparkSession;

public class ExportToMySQL extends Export {
    public ExportToMySQL(SparkSession spark, Integer timeType, String timeID, Integer backDate, String dbID, Integer frequency) {
        super(spark, timeType, timeID, backDate, dbID, frequency);
    }

    public void dpD() {
        exeSQLFile("mysql_d.sql", "export");
    }
}
