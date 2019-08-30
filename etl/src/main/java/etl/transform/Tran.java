package etl.transform;

import etl.pub.Func;
import etl.pub.Transform;
import org.apache.spark.sql.SparkSession;

public class Tran extends Transform {
    private String sqlDir = Func.getTransformSqlDir();

    public Tran(SparkSession session, Integer timeType, String timeID, Integer backDate, Integer frequency) {
        super(session, timeType, timeID, backDate, frequency);
    }

    public void s2iD() {
        exeSqls(Func.readSqlFile(sqlDir + "s2m_d.sql"));
    }
}
