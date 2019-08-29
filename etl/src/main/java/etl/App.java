package etl;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.LocalDate;

/**
 * Hello world!
 */
public final class App {
    private App() {
    }

    private static void job(String jobName, String master, Integer timeType, String timeID, Integer backDate) {
        Dispatch disp = new Dispatch(master, timeType, timeID, backDate);
        switch (jobName) {
            case "prod":
                disp.prod();
        }
    }

    public static void main(String[] args) {
        final Logger logger = LogManager.getLogger();
        logger.debug("start");
//        final String MASTER = "spark://192.168.1.39:7077";
        final String MASTER = "yarn";
        Integer backDate = 7;
        String jobName = "prod";
        int timeType = 1;
        String timeID = LocalDate.now().plusDays(-1).toString();

        if (args.length == 4) {
            jobName = args[1];
            timeType = Integer.parseInt(args[2]);
            timeID = args[3];
        }
        job(jobName, MASTER, timeType, timeID, backDate);
    }
}
