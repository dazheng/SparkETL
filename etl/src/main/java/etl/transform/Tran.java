package etl.transform;

import etl.pub.Transform;
import org.apache.spark.sql.SparkSession;

public class Tran extends Transform {
    public Tran(SparkSession spark, Integer timeType, String timeID, Integer backDate, Integer frequency) {
        super(spark, timeType, timeID, backDate, frequency);
    }

    public void s2iD() {
        exeSQLFile("s2m_d.sql", "insert");
    }
}

