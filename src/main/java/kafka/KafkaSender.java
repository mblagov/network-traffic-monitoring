package kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.IOException;
import java.util.Properties;

public class KafkaSender {

    private final Properties properties;

    public KafkaSender() {
        // Set properties used to configure the producer
        this.properties = new Properties();

        properties.put("bootstrap.servers", "localhost:2181");
        // Set how to serialize key/value pairs
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("retries", 3);
        //Reduce the no of requests less than 0
        properties.put("linger.ms", 1);
    }

    public void send(String alert) throws IOException {
        KafkaProducer producer = new KafkaProducer<>(this.properties);
        try
        {
            Object metadata = producer.send(new ProducerRecord<>("alerts", alert)).get();
        }
        catch (Exception ex)
        {
            System.err.print(ex.getMessage());
            throw new IOException(ex.toString());
        }
        producer.close();
    }
}
