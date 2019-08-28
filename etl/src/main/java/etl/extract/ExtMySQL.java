package etl.extract;

import etl.pub.Extract;
import etl.pub.Func;
import org.apache.spark.sql.SparkSession;

public class ExtMySQL extends Extract {
    private String sqlDir = Func.getExtractSqlDir();

    public ExtMySQL(SparkSession session, Integer timeType, String timeID, Integer backDate, String dbID, Integer frequency) {
        super(session, timeType, timeID, backDate, dbID, frequency);
    }

    public void dpFull() {
        exeExtractSqls(Func.readSqlFile(this.sqlDir + "dp.sql"));
    }
}
