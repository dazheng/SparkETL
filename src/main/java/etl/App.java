package etl;

import etl.utils.Public;
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
        logger.info(Public.getEqualSep());

        Integer backDate = 1; // 回溯天数，支持一次计算多天
        String jobName = "prod";
        Integer timeType = 1; // time_type: 1 日； 2 周；3 月； 4 年；11 小时； 12 半小时； 13 10分钟； 14 5分钟； 15 1分钟
        String timeID = LocalDate.now().plusDays(-1).toString();

        if (args.length == 3) {
            jobName = args[0].toLowerCase();
            timeType = Integer.parseInt(args[1]);
            timeID = args[2];
        }

        job(jobName, timeType, timeID, backDate);
    }
}
