package etl.pub;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.List;

public class Transform extends ETL {
    private final Logger logger = LogManager.getLogger();
    public Transform(SparkSession session, Integer timeType, String timeID, Integer backDate, Integer frequency) {
        super(session, timeType, timeID, backDate, frequency);
    }

    public void dropHivePartition(List<String> tables){
        for(String t:tables) {
                String start =getStartTimeID();
            String end  = getEndTimeID();
                while (start.compareTo(end) <=0 ) {
                    String sql = String.format("alter table %s drop if exists partition(time_type=%s, time_id='%s')", t, getTimeType(), start);
                    exeSql(sql);
                    start = getNextTimeID(start);
                }
            }
    }
    private void exeDirSql(String dir, String sql){
            if (dir!=null) {
                logger.info(dir);
            }
            Dataset<Row> df= exeSql(sql);
            if (dir !=null){
                toLocalDir(df,dir);
            }

    }

   protected void exeSqls(String sqls){
        exeSqls(sqls, this::exeDirSql, 1);
   }

   public void exeViewSqls(String sqls){
        exeSqls(sqls, this::sqlSpecialView,2);
   }

   public void release(){

   }
}
