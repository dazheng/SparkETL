package etl;

import etl.pub.Func;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDate;
import java.time.LocalDateTime;


public final class App {
    private static void job(String jobName, Integer timeType, String timeID, Integer backDate) {
        Dispatch disp = new Dispatch(timeType, timeID, backDate);
        switch (jobName) {
            case "prod":
                disp.prod();
                break;
            default:
                break;
        }
    }

    public static void main(String[] args) {
        final Logger logger = LoggerFactory.getLogger(App.class);
        LocalDateTime start = LocalDateTime.now();
        logger.info(Func.getEqualSep());
        logger.info("start");
        Integer backDate = 7;
        String jobName = "prod";
        int timeType = 1;
        String timeID = LocalDate.now().plusDays(-1).toString();

        if (args.length == 4) {
            jobName = args[1].toLowerCase();
            timeType = Integer.parseInt(args[2]);
            timeID = args[3];
        }
        job(jobName, timeType, timeID, backDate);
    }
}
