package etl.utils;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
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

    /**
     * 删除hive对应分区
     *
     * @param tables 表名列表
     */
    public void dropHivePartition(List<String> tables) throws Exception {
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

    /**
     * 执行SQL，如果需要导出就导出到本地
     *
     * @param dir 导出的本地目录
     * @param sql sql语句
     * @throws IOException
     */
    private void exeSQL(String dir, String sql) throws IOException {
        if (dir != null && !dir.isEmpty()) {
            logger.info(dir);
        }
        Dataset<Row> df = exeSQL(sql);
        if (dir != null && !dir.isEmpty()) {
            toLocalDirectory(df, dir);
        }
    }

    /**
     * 根据类型执行sql文件内容
     *
     * @param fileName sql文件名
     * @param exeType  执行类型
     * @throws Exception
     */
    protected void exeSQLFile(String fileName, String exeType) throws Exception {
        String sqls = Public.readSqlFile(getTransformSqlDir() + fileName);
        exeType = exeType.toLowerCase();
        if ("insert".equals(exeType)) {
            exeSQLs(sqls, Public.rethrowBiConsumer(this::exeSQL), 1);
        } else if ("view".equals(exeType)) {
            exeSQLs(sqls, this::sqlToSpecialView, 2);
        } else {
            this.logger.error("not support {}", exeType);
            throw new Exception("not support " + exeType);
        }
    }
}
