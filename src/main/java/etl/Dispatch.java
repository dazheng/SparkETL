package etl;

import etl.export.ExportToDBDaily;
import etl.extract.ExtractFromDBDaily;
import etl.transform.Tran;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.LocalDateTime;

/**
 * 批处理调度
 */
public class Dispatch {
    private final Logger logger = LoggerFactory.getLogger(Dispatch.class);
    private final Integer timeType;
    private final String timeID;
    private SparkSession spark;
    private Integer backDate;

    Dispatch(Integer timeType, String timeID, Integer backDate) {
        this.timeType = timeType;
        this.timeID = timeID;
        this.backDate = backDate;
    }

    /**
     * 天粒度数据获取
     *
     * @throws Exception
     */
    private void extractD() throws Exception {
        ExtractFromDBDaily dp = new ExtractFromDBDaily(this.spark, this.timeType, this.timeID, this.backDate,
            "mysql", this.timeType);
        dp.dpFull();
        dp.release();
    }

    /**
     * 天粒度转换
     *
     * @throws Exception
     */
    private void transformD() throws Exception {
        Tran t = new Tran(this.spark, this.timeType, this.timeID, this.backDate, this.timeType);
        t.s2iD();
        t.i2mD();
        t.release();
    }


    /**
     * 天粒度数据导出及导出后的计算
     *
     * @throws Exception
     */
    private void exportD() throws Exception {
        ExportToDBDaily dp = new ExportToDBDaily(this.spark, this.timeType, this.timeID, this.backDate,
            "es", this.timeType);
        dp.dpD();
        dp.release();
    }

    private void setSpark(String appName) {
        this.spark = SparkSession.builder().appName(appName).enableHiveSupport().getOrCreate();
    }

    /**
     * 正式的任务
     */
    public void prod() {
        String appName = "";
        LocalDateTime start = LocalDateTime.now();
        try {
            switch (this.timeType) {
                case 1:
                    appName = "etl_day";
                    startLog(appName);
                    setSpark(appName);
                    this.backDate = 1;
                    extractD();
                    transformD();
                    exportD();
                    break;

                case 11:
                    appName = "etl_1hour";
                    startLog(appName);
                    setSpark(appName);
                    break;

            }
        } catch (Exception e) {
            this.logger.error(e.toString(), e);
        } finally {
            this.spark.stop();
            this.logger.info("timeID={} appName={} time taken {} s",
                timeID, appName, Duration.between(start, LocalDateTime.now()).getSeconds());
        }
    }

    public void startLog(String appName) {
        this.logger.info("timeID={} appName={} start", timeID, appName);
    }
}
