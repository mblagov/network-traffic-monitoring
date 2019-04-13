package SparkStreaming;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.Minutes;
import org.apache.spark.streaming.Seconds;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

public class Spark implements Runnable {
    JavaDStream<HostTraffic> stream;
    JavaStreamingContext jssc;

    public Spark() {
        SparkConf sparkConf = new SparkConf()
                .setAppName("network-traffic-monitoring")
                .setMaster("local[2]");
        jssc = new JavaStreamingContext(sparkConf, Durations.seconds(1));
        stream = jssc
                .receiverStream(new TrafficReceiver());
        stream = stream.window(Minutes.apply(5), Seconds.apply(1));

    }


    @Override
    public void run() {

        jssc.start();
        try {
            jssc.awaitTermination();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
