import Models.HostTraffic;
import Models.TrafficReceiver;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Seconds;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

public class SparkService implements Runnable {

    private JavaSparkContext sparkContext;
    private JavaDStream<HostTraffic> stream;
    private JavaStreamingContext streamingContext;

    public SparkService() {
        SparkConf sparkConf = new SparkConf().setAppName("network-traffic-monitoring")
                .setMaster("local[*]");
        sparkContext = new JavaSparkContext(sparkConf);
        streamingContext = new JavaStreamingContext(
                sparkContext, Seconds.apply(1));
        streamingContext.checkpoint("file:///home/students/network-traffic-monitoring/checkpoints");
        stream = streamingContext
                .receiverStream(new TrafficReceiver());
    }

    public void run() {
        streamingContext.start();
        try {
            streamingContext.awaitTermination();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public JavaSparkContext getSparkContext() {
        return sparkContext;
    }

    public void setSparkContext(JavaSparkContext sparkContext) {
        this.sparkContext = sparkContext;
    }

    public JavaDStream<HostTraffic> getStream() {
        return stream;
    }

    public void setStream(JavaDStream<HostTraffic> stream) {
        this.stream = stream;
    }
}
