package Models;

import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.receiver.Receiver;
import org.pcap4j.core.*;
import org.pcap4j.util.NifSelector;

import java.io.IOException;

public class TrafficReceiver extends Receiver<HostTraffic> {

    private PcapHandle handle;
    final int snapshotLength = 65536; // in bytes
    final int readTimeout = 50; // in milliseconds

    public TrafficReceiver() {
        super(StorageLevel.MEMORY_AND_DISK());
    }

    @Override
    public void onStart() {
        PcapNetworkInterface device = getNetworkDevice();
        System.out.println("You chose: " + device);

        if (device == null) {
            System.out.println("No device chosen.");
            System.exit(1);
        }

        // Open the device and get a handle
        try {
            handle = device.openLive(snapshotLength, PcapNetworkInterface.PromiscuousMode.PROMISCUOUS, readTimeout);
        } catch (PcapNativeException e) {
            e.printStackTrace();
        }

        // Create a listener that defines what to do with the received packets
        PacketListener listener = packet -> {
            HostTraffic hostTraffic = new HostTraffic();
            hostTraffic.setTrafficAmount(packet.length());
            TrafficReceiver.this.store(hostTraffic);
        };

        // run in new thread to separate capturing of traffic from main thread
        Thread thread = new Thread(() -> {
            try {
                int maxPackets = -1; //infinite loop
                handle.loop(maxPackets, listener);
            } catch (NotOpenException | PcapNativeException | InterruptedException e) {
                e.printStackTrace();
            }
        });

        thread.start();
    }

    @Override
    public void onStop() {
        handle.close();
    }

    static PcapNetworkInterface getNetworkDevice() {
        PcapNetworkInterface device = null;
        try {
            device = new NifSelector().selectNetworkInterface();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return device;
    }
}
