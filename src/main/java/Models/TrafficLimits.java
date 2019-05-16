package Models;
import java.io.Serializable;
import java.util.List;

import org.apache.spark.sql.Row;

public class TrafficLimits implements Serializable {
    private static final String MAX_LIMIT_NAME = "max";
    private static final String MIN_LIMIT_NAME = "min";
    public static Integer DEFAULT_MIN = 1024;
    public static Integer DEFAULT_MAX = 1048576;

    private Integer minLimit = DEFAULT_MIN;
    private Integer maxLimit = DEFAULT_MAX;

    public static TrafficLimits createWithLimits(List<Row> rowList) {
        TrafficLimits result = new TrafficLimits();
        for (Row r : rowList) {
            if (MIN_LIMIT_NAME.equals(r.getString(0))) {
                result.minLimit = r.getInt(1);
            } else if (MAX_LIMIT_NAME.equals(r.getString(0))) {
                result.maxLimit = r.getInt(1);
            }
        }
        return result;
    }

    public Integer getMinLimit() {
        return minLimit;
    }

    public Integer getMaxLimit() {
        return maxLimit;
    }
}
