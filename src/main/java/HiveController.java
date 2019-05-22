import Models.TrafficLimits;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.hive.HiveContext;

import java.util.List;

public class HiveController {

    private HiveContext hiveContext;

    public HiveController(JavaSparkContext sparkContext) {
        hiveContext = new HiveContext(sparkContext);
    }

    public TrafficLimits select() {
        List<Row> rowList = hiveContext.table("traffic_limits.limits_per_hour")
                .collectAsList();
        TrafficLimits trafficLimits = TrafficLimits
                .createWithLimits(rowList);
        return trafficLimits;
    }
}
