package etl;

import etl.export.ExportToMySQL;
import etl.extract.ExtractFromMySQL;
import etl.transform.Tran;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.LocalDateTime;

class Dispatch {
    private static final Logger logger = LoggerFactory.getLogger(Dispatch.class);
    private final Integer timeType;
    private final String timeID;
    private SparkSession spark;
    private Integer backDate;

    Dispatch(Integer timeType, String timeID, Integer backDate) {
        this.timeType = timeType;
        this.timeID = timeID;
        this.backDate = backDate;
    }

    private void extractD() {
        ExtractFromMySQL dp = new ExtractFromMySQL(this.spark, this.timeType, this.timeID, this.backDate, "dp", this.timeType);
        dp.dpFull();
        dp.release();
    }

    private void transformD() {
        Tran t = new Tran(this.spark, this.timeType, this.timeID, this.backDate, this.timeType);
        t.s2iD();
        t.i2mD();
        t.release();
    }

    private void exportD() {
        ExportToMySQL dp = new ExportToMySQL(this.spark, this.timeType, this.timeID, this.backDate, "dp", this.timeType);
        dp.dpD();
        dp.release();
    }

    private void setSpark(String appName) {
        this.spark = SparkSession.builder().appName(appName).enableHiveSupport().getOrCreate();
    }

    public void prod() {
        String appName = "";
        LocalDateTime start = LocalDateTime.now();
        try {
            switch (this.timeType) {
                case 1:
                    appName = "etl_day";
                    setSpark(appName);
                    this.backDate = 1;
                    extractD();
                    transformD();
                    exportD();
                    break;
                case 11:
                    appName = "etl_1hour";
                    setSpark(appName);
                    break;
            }
        } catch (Exception e) {
            logger.error(e.toString(), e);
        } finally {
            this.spark.stop();
            logger.info("{} time taken {} s", appName, Duration.between(start, LocalDateTime.now()).getSeconds());
        }
    }
}
