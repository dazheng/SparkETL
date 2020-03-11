package etl;

import etl.export.ExpMySQL;
import etl.extract.ExtMySQL;
import etl.transform.Tran;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.sql.SparkSession;

import java.time.LocalDateTime;

class Dispatch {
    private final Logger logger = LogManager.getLogger();
    private final String master;
    private final Integer timeType;
    private final String timeID;
    private SparkSession spark;
    private Integer backDate;

    Dispatch(String master, Integer timeType, String timeID, Integer backDate) {
        this.master = master;
        this.timeType = timeType;
        this.timeID = timeID;
        this.backDate = backDate;
    }

    private void extractD() {
        ExtMySQL dp = new ExtMySQL(this.spark, this.timeType, this.timeID, this.backDate, "dp", this.timeType);
        dp.dpFull();
        dp.release();
    }

    private void transformD() {
        Tran t = new Tran(this.spark, this.timeType, this.timeID, this.backDate, this.timeType);
        t.s2iD();
        t.release();
    }

    private void exportD() {
        ExpMySQL dp = new ExpMySQL(this.spark, this.timeType, this.timeID, this.backDate, "dp", this.timeType);
        dp.dpD();
        dp.release();
    }

    private void setSpark(String appName) {
        this.spark = SparkSession.builder().appName(appName).enableHiveSupport().getOrCreate();
        // this.session = SparkSession.builder().master(this.master).appName(appName).enableHiveSupport().getOrCreate();
    }

    void prod() {
        String appName = "";
        LocalDateTime start = LocalDateTime.now();
//        try {
            switch (this.timeType) {
                case 1:
                    setSpark("etl_day");
                    this.backDate = 7;
//                    extractD();
//                    transformD();
                    exportD();
                    break;
                case 11:
                    setSpark("etl_1hour");
                    break;
            }
//        } catch (Exception e) {
//            logger.fatal(e);
//        } finally {
//            this.session.stop();
//            logger.warn("%s time taken: %d s", appName, Duration.between(LocalDateTime.now(), start).getSeconds());
//        }
    }
}
