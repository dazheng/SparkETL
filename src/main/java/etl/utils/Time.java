package etl.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.Map;

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
                startDateTime = timeID;
                DateTimeFormatter formatter2 = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm");
                endDateTime = dateTime.plus(-(this.backDate - 1), ChronoUnit.MINUTES).format(formatter2) + ":59";
                timeID = dateTime.format(dateFormatter);
                startDateTime = timeID + " 00:00:00";
                endDateTime = timeID + " 23:59:59";
                break;
        }
        Map<String, String> map = new HashMap<>();
        map.put("v_time_type", timeType);
        map.put("v_time_id", timeID);
        map.put("v_start_time_id", startTimeID);
        map.put("v_start_date_id", startDateID);
        map.put("v_end_date_id", endDateID);
        map.put("v_start_date_time", startDateTime);
        map.put("v_end_date_time", endDateTime);
        return map;
    }

    protected String getNextTimeID(String timeID) {
        switch (this.timeType) {
            case 1:
                return LocalDate.parse(this.timeID, this.dateTimeFormatter).plusDays(1).format(this.dateFormatter);
            case 2:
                return LocalDate.parse(this.timeID, this.dateTimeFormatter).plusDays(7).format(this.dateFormatter);
            case 3:
                return LocalDate.parse(this.timeID, this.monthFormatter).plusMonths(1).format(this.dateFormatter);
            default:
                this.logger.error("not support time_type={}", this.timeType);
                return timeID;
        }
    }

    private String getTimeParameter(String varTime) {
        return this.timeParas.get(varTime);
    }

    protected String getStartTimeID() {
        String start = "";
        switch (this.timeType) {
            case 1:
                start = getTimeParameter("v_start_time_id");
                break;
            case 2:
                start = getTimeParameter("v_time_id");
                break;
            case 3:
                start = getTimeParameter("v_time_id");
                break;
        }
        return start;
    }

    protected String getEndTimeID() {
        String end = "";
        switch (this.timeType) {
            case 1:
                end = getTimeParameter("v_end_time_id");
                break;
            case 2:
                end = getTimeParameter("v_time_id");
                break;
            case 3:
                end = getTimeParameter("v_time_id");
                break;
        }
        return end;
    }
}
