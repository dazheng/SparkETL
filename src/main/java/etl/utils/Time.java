package etl.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;

/**
 * 时间相关处理
 */
public class Time {
    private final Logger logger = LoggerFactory.getLogger(Time.class);
    private final Integer timeType;
    private final String timeID;
    private final Integer backDate;
    private final Integer frequency;
    private Map<String, String> timeParas = new HashMap<String, String>();

    private DateTimeFormatter dateFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
    private DateTimeFormatter monthFormatter = DateTimeFormatter.ofPattern("yyyy-MM");
    private DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    Time(Integer timeType, String timeID, Integer backDate, Integer frequency) {
        this.timeType = timeType;
        this.timeID = timeID;
        this.backDate = backDate;
        this.frequency = frequency;
        this.timeParas = generateTimeParameters();
    }

    protected Integer getTimeType() {
        return timeType;
    }

    protected String getTimeID() {
        return timeID;
    }

    protected Integer getBackDate() {
        return backDate;
    }

    protected Integer getFrequency() {
        return frequency;
    }

    protected Map<String, String> getTimeParameters() {
        return timeParas;
    }

    /**
     * 根据传入的timeType,timeID生成时间相关的参数
     *
     * @return 时间参数map
     */
    private Map<String, String> generateTimeParameters() {
        String startTimeID = "";
        String startDateID = "";
        String endDateID = "";
        String startDateTime = "";
        String endDateTime = "";
        String timeType = String.valueOf(this.timeType);
        String timeID = this.timeID;

        switch (this.timeType) { // time_type: 1 日； 2 周；3 月； 4 年；11 小时； 12 半小时； 13 10分钟； 14 5分钟； 15 1分钟
            case 1:
                LocalDate date = LocalDate.parse(this.timeID, this.dateFormatter);
                startTimeID = date.plusDays(-(this.backDate - 1)).format(this.dateFormatter);
                startDateTime = startTimeID + " 00:00:00";
                endDateTime = this.timeID + " 23:59:59";
                startDateID = startTimeID;
                endDateID = this.timeID;
                break;
            case 11:
                LocalDate dateTime = LocalDate.parse(this.timeID, this.dateTimeFormatter);
                timeID = dateTime.format(dateFormatter);
                startDateTime = timeID + " 00:00:00";
                endDateTime = timeID + " 23:59:59";
                break;
        }
        Map<String, String> map = new HashMap<>();
        map.put("time_type", timeType);
        map.put("time_id", timeID);
        map.put("start_time_id", startTimeID);
        map.put("start_date_id", startDateID);
        map.put("end_date_id", endDateID);
        map.put("start_date_time", startDateTime);
        map.put("end_date_time", endDateTime);
        return map;
    }


    /**
     * 得到下一个timeID
     *
     * @param timeID
     * @return
     */
    protected String getNextTimeID(String timeID) throws Exception {
        switch (this.timeType) {
            case 1:
                return LocalDate.parse(this.timeID, this.dateTimeFormatter).plusDays(1).format(this.dateFormatter);
            case 2:
                return LocalDate.parse(this.timeID, this.dateTimeFormatter).plusDays(7).format(this.dateFormatter);
            case 3:
                return LocalDate.parse(this.timeID, this.monthFormatter).plusMonths(1).format(this.dateFormatter);
            default:
                this.logger.error("not support time_type={}", this.timeType);
                throw new Exception("not support " + String.valueOf(this.timeType));
        }
    }

    private String getTimeParameter(String varTime) {
        return this.timeParas.get(varTime);
    }

    /**
     * 获取开始的timeID
     *
     * @return timeID
     */
    protected String getStartTimeID() {
        String start = "";
        switch (this.timeType) {
            case 1:
                start = getTimeParameter("start_time_id");
                break;
            case 2:
                start = getTimeParameter("time_id");
                break;
            case 3:
                start = getTimeParameter("time_id");
                break;
        }
        return start;
    }


    /**
     * 获取结束的timeID
     *
     * @return timeID
     */
    protected String getEndTimeID() {
        String end = "";
        switch (this.timeType) {
            case 1:
                end = getTimeParameter("time_id");
                break;
            case 2:
                end = getTimeParameter("time_id");
                break;
            case 3:
                end = getTimeParameter("time_id");
                break;
        }
        return end;
    }
}
