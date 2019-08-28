package etl;

import etl.export.ExpMySQL;
import etl.extract.ExtMySQL;
import etl.transform.Tran;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.sql.SparkSession;

import java.time.Duration;
import java.time.LocalDateTime;

class Dispatch {
    private final Logger logger = LogManager.getLogger();
    private final String master;
    private SparkSession session;
    private final Integer timeType;
    private final String timeID;
    private Integer backDate;

    Dispatch(String master, Integer timeType, String timeID, Integer backDate) {
        this.master = master;
        this.timeType = timeType;
        this.timeID = timeID;
        this.backDate = backDate;
    }

    private void extractD() {
        ExtMySQL dp = new ExtMySQL(this.session, this.timeType, this.timeID, this.backDate, "dp", this.timeType);
        dp.dpFull();
        dp.release();
    }

    private void transformD() {
        Tran t = new Tran(this.session, this.timeType, this.timeID, this.backDate, this.timeType);
        t.s2iD();
        t.release();
    }

    private void exportD() {
        ExpMySQL dp = new ExpMySQL(this.session, this.timeType, this.timeID, this.backDate, "dp", this.timeType);
        dp.dpD();
        dp.release();
    }

    private void setSession(String appName) {
        this.session = SparkSession.builder().master(this.master).appName(appName).enableHiveSupport().getOrCreate();
    }

    void prod() {
        String appName = "";
        LocalDateTime start = LocalDateTime.now();
        try {
            switch (this.timeType) {
                case 1:
                    setSession("etl_day");
                    this.backDate = 15;
                    extractD();
                    transformD();
                    exportD();
                case 11:
                    setSession("etl_1hour");
            }
        } catch (Exception e) {
            logger.error(e);
        } finally {
            this.session.stop();
            logger.warn("%s time taken: %d s", appName, Duration.between(LocalDateTime.now(), start).getSeconds());
        }
    }
}

