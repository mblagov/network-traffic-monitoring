import Models.HostTraffic;
import Models.TrafficReceiver;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.Minutes;
import org.apache.spark.streaming.Seconds;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.pcap4j.core.PcapNetworkInterface;
import org.pcap4j.util.NifSelector;

import java.io.IOException;

public class Main {

    static PcapNetworkInterface getNetworkDevice() {
        PcapNetworkInterface device = null;
        try {
            device = new NifSelector().selectNetworkInterface();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return device;
    }

    public static void main(String[] args) {

        SparkConf sparkConf = new SparkConf()
                .setAppName("network-traffic-monitoring")
                .setMaster("local[2]");
        JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.seconds(1));
        JavaDStream<HostTraffic> stream = jssc
                .receiverStream(new TrafficReceiver());
        jssc.checkpoint("file:///home/students/network-traffic-monitoring/checkpoints");
        stream = stream.window(Minutes.apply(5), Seconds.apply(1));
    }

}
