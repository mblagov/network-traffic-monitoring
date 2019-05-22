package Models;

import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.receiver.Receiver;

public class TrafficReceiverMock extends Receiver<HostTraffic> {

    public TrafficReceiverMock() {
        super(StorageLevel.MEMORY_AND_DISK());
    }

    @Override
    public void onStart() {

        Thread thread = new Thread(() -> {
            HostTraffic hostTraffic = new HostTraffic();
            hostTraffic.setTrafficAmount((int) Math.random() * 50);
            TrafficReceiverMock.this.store(hostTraffic);
        });

        thread.start();
    }

    @Override
    public void onStop() {

    }

}
