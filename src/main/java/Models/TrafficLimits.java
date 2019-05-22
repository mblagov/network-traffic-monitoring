package Models;

import org.apache.spark.sql.Row;
import scala.Tuple3;

import java.io.Serializable;
import java.sql.Timestamp;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.function.Predicate;

public class TrafficLimits implements Serializable {
    private static final String MAX_LIMIT_NAME = "max";
    private static final String MIN_LIMIT_NAME = "min";
    private static final String EFFECTIVE_DATE = "effective_date";
    public static Integer DEFAULT_MIN = 1024;
    public static Integer DEFAULT_MAX = 1048576;
    public static Timestamp DEFAULT_EFFECTIVE_DATE_MAX = Timestamp.valueOf("2019-05-14 13:57:15");
    public static Timestamp DEFAULT_EFFECTIVE_DATE_MIN = Timestamp.valueOf("2019-05-14 13:57:15");

    private Integer minLimit = DEFAULT_MIN;
    private Integer maxLimit = DEFAULT_MAX;
    private Timestamp effectiveDateMax = DEFAULT_EFFECTIVE_DATE_MAX;
    private Timestamp effectiveDateMin = DEFAULT_EFFECTIVE_DATE_MIN;

    public static TrafficLimits createWithLimits(List<Row> rowList) {
        TrafficLimits result = new TrafficLimits();
        Predicate<Row> isMin = x -> MIN_LIMIT_NAME.equals(x.getString(0));
        Predicate<Row> isMax = x -> MAX_LIMIT_NAME.equals(x.getString(0));
        Comparator<Row> byTimestamp = Comparator.comparing(row -> row.getTimestamp(2));
        System.out.println("START OF ");
        Optional<Row> maxRow = rowList.stream().filter(isMax).max(byTimestamp);
        Optional<Row> minRow = rowList.stream().filter(isMin).max(byTimestamp);
        System.out.println("MAX ROW IS " + maxRow.isPresent());
        System.out.println("MAX ROW IS " + maxRow.get());

        if (maxRow.isPresent()) {
            result.maxLimit = maxRow.get().getInt(1);
            result.effectiveDateMax = maxRow.get().getTimestamp(2);
        }
        if (minRow.isPresent()) {
            result.minLimit = minRow.get().getInt(1);
            result.effectiveDateMin = minRow.get().getTimestamp(2);
        }
        return result;
    }

    public Integer getMinLimit() {
        return minLimit;
    }

    public Integer getMaxLimit() {
        return maxLimit;
    }

    public Timestamp getEffectiveDateMax() {
        return effectiveDateMax;
    }

    public void setEffectiveDateMax(Timestamp effectiveDateMax) {
        this.effectiveDateMax = effectiveDateMax;
    }

    public Timestamp getEffectiveDateMin() {
        return effectiveDateMin;
    }

    public void setEffectiveDateMin(Timestamp effectiveDateMin) {
        this.effectiveDateMin = effectiveDateMin;
    }
}
