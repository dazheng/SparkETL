package etl.utils;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class Transform extends ETL {
    private final Logger logger = LoggerFactory.getLogger(Transform.class);

    public Transform(SparkSession spark, Integer timeType, String timeID, Integer backDate, Integer frequency) {
        super(spark, timeType, timeID, backDate, frequency);
    }

    public static String getTransformSqlDir() {
        return "transform/";
    }

    public void release() {

    }

    public void dropHivePartition(List<String> tables) {
        for (String t : tables) {
            String start = getStartTimeID();
            String end = getEndTimeID();
            while (start.compareTo(end) <= 0) {
                String sql = String.format("alter table %s drop if exists partition(time_type=%s, time_id='%s')", t, getTimeType(), start);
                exeSQL(sql);
                start = getNextTimeID(start);
            }
        }
    }

    private void exeSQL(String dir, String sql) {
        if (dir != null && !dir.isEmpty()) {
            logger.info(dir);
        }
        Dataset<Row> df = exeSQL(sql);
        if (dir != null && !dir.isEmpty()) {
            toLocalDirectory(df, dir);
        }
    }

    protected void exeSQLFile(String fileName, String exeType) {
        String sqls = Public.readSqlFile(getTransformSqlDir() + fileName);
        exeType = exeType.toLowerCase();
        if (exeType.equals("insert")) {
            exeSQLs(sqls, this::exeSQL, 1);
        } else if (exeType.equals("view")) {
            exeSQLs(sqls, this::sqlToSpecialView, 2);
        }
    }
}
