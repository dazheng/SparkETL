package etl.transform;

import etl.utils.Transform;
import org.apache.spark.sql.SparkSession;

public class Tran extends Transform {
    public Tran(SparkSession spark, Integer timeType, String timeID, Integer backDate, Integer frequency) {
        super(spark, timeType, timeID, backDate, frequency);
    }

    public void s2iD() {
        exeSQLFile("s2i_d.sql", "insert");
    }

    public void i2mD() {
        exeSQLFile("i2m_d.sql", "insert");
    }
}

