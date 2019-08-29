package etl.export;

import etl.pub.Export;
import etl.pub.Func;
import org.apache.spark.sql.SparkSession;

public class ExpMySQL extends Export {
    private String sqlDir = Func.getExportSqlDir();

    public ExpMySQL(SparkSession session, Integer timeType, String timeID, Integer backDate, String dbID, Integer frequency) {
        super(session, timeType, timeID, backDate, dbID, frequency);
    }

    public void dpD() {
        exeLoadSqls(Func.readSqlFile(this.sqlDir + "dp_d.sql"));
    }
}
