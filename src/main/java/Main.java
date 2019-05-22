import Models.HostTraffic;
import Models.TrafficLimits;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Minutes;
import org.apache.spark.streaming.Seconds;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import scala.Tuple2;
import scala.Tuple3;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class Main {

    static Logger logger = LogManager.getRootLogger();

    public static void main(String[] args) {

        SparkService sparkService = new SparkService();

        JavaSparkContext sparkContext = sparkService.getSparkContext();
        HiveController hiveController = new HiveController(sparkContext);
        ScheduledTrafficLimits scheduledTrafficLimits = new ScheduledTrafficLimits(hiveController);
        scheduledTrafficLimits.run();

        final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
        scheduler.scheduleAtFixedRate(scheduledTrafficLimits, 6, 6, TimeUnit.MINUTES);

        logger.info("TRAFFIC_LIMITS_DATE: " + scheduledTrafficLimits.trafficLimits.getEffectiveDateMax());

        final Map<String, Object> kafkaProperties = new HashMap<>();
        kafkaProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                "t510:9092");
        kafkaProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringSerializer");
        kafkaProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringSerializer");

        JavaDStream<HostTraffic> stream = sparkService.getStream();
        stream = stream.window(Minutes.apply(5), Seconds.apply(1));

        JavaPairDStream<String, Long> pdStream = stream
                .mapToPair(new TrafficToPairMapper());
        JavaPairDStream<String, Tuple3<Long, AlertType, Boolean>> pdStreamWithState = pdStream
                .updateStateByKey(new StateUpdaterFunction2(scheduledTrafficLimits.trafficLimits));
        pdStreamWithState.filter(new FiringEventsFilter()).map(new KeyMapper())
                .foreachRDD(new KafkaNotifier(kafkaProperties));


        sparkService.run();

    }

    private static class ScheduledTrafficLimits implements Runnable {

        private HiveController hiveController;
        private TrafficLimits trafficLimits;

        ScheduledTrafficLimits(HiveController hiveController) {

            this.hiveController = hiveController;
        }

        public void run() {
            System.out.println("START OF RUN");
            trafficLimits = hiveController.select();
            logger.info("TRAFFIC_LIMITS_DATE: " + trafficLimits.getEffectiveDateMax());
        }
    }

    private static final class KafkaNotifier implements
            VoidFunction<JavaRDD<Tuple3<Long, AlertType, Boolean>>> {
        private final Map<String, Object> kafkaProperties;

        private KafkaNotifier(Map<String, Object> kafkaProperties) {
            this.kafkaProperties = kafkaProperties;
        }

        @Override
        public void call(JavaRDD<Tuple3<Long, AlertType, Boolean>> finalEvenRDD) {
            if (!finalEvenRDD.isEmpty()) {
                // we always have single event since handling single ip address
                Tuple3<Long, AlertType, Boolean> eventToProduce = finalEvenRDD
                        .first();

                Long trafficAmount = eventToProduce._1();
                AlertType alertType = eventToProduce._2();
                Boolean fireEvent = eventToProduce._3();
                if (fireEvent) {
                    KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(
                            kafkaProperties);
                    List<ProducerRecord<String, String>> producerRecords = createProducerRecords(
                            trafficAmount, alertType);
                    for (ProducerRecord<String, String> producerRecord : producerRecords) {
                        kafkaProducer.send(producerRecord);
                    }
                    kafkaProducer.close();
                }
            }
        }

        private List<ProducerRecord<String, String>> createProducerRecords(
                Long trafficAmount, AlertType alertType) {

            List<ProducerRecord<String, String>> result = new ArrayList<>();
            StringBuilder messageBuilder = new StringBuilder();
            ProducerRecord<String, String> producerRecord;
            if (AlertType.HIGH_FROM_LOW == alertType
                    || AlertType.LOW_FROM_HIGH == alertType) {
                messageBuilder.append("Alert is gone. Traffic is OK.");
                producerRecord = new ProducerRecord<>(
                        "alerts", messageBuilder.toString());
                result.add(producerRecord);
                messageBuilder = new StringBuilder();
            }

            if (AlertType.HIGH == alertType
                    || AlertType.HIGH_FROM_LOW == alertType) {
                messageBuilder.append("New alert! Traffic rate is too high!");
                messageBuilder.append(" Current traffic amount is "
                        + trafficAmount);
            }
            if (AlertType.LOW == alertType
                    || AlertType.LOW_FROM_HIGH == alertType) {
                messageBuilder.append("New alert! Traffic rate is too low!");
                messageBuilder.append(" Current traffic amount is "
                        + trafficAmount);
            }
            if (AlertType.NORMAL_FROM_HIGH == alertType
                    || AlertType.NORMAL_FROM_LOW == alertType) {
                messageBuilder.append("Alert is gone. Traffic is OK.");
            }

            producerRecord = new ProducerRecord<>("alerts",
                    messageBuilder.toString());
            result.add(producerRecord);
            return result;
        }
    }

    private static final class KeyMapper
            implements
            Function<Tuple2<String, Tuple3<Long, AlertType, Boolean>>, Tuple3<Long, AlertType, Boolean>> {
        @Override
        public Tuple3<Long, AlertType, Boolean> call(
                Tuple2<String, Tuple3<Long, AlertType, Boolean>> v1) {
            return v1._2();
        }
    }


    private static final class FiringEventsFilter implements
            Function<Tuple2<String, Tuple3<Long, AlertType, Boolean>>, Boolean> {
        @Override
        public Boolean call(Tuple2<String, Tuple3<Long, AlertType, Boolean>> v1) {
            return v1._2()._3();
        }
    }

    private static final class StateUpdaterFunction2
            implements
            Function2<List<Long>, Optional<Tuple3<Long, AlertType, Boolean>>, Optional<Tuple3<Long, AlertType, Boolean>>> {

        private final TrafficLimits trafficLimits;

        private StateUpdaterFunction2(TrafficLimits trafficLimits) {
            this.trafficLimits = trafficLimits;
        }

        @Override
        public Optional<Tuple3<Long, AlertType, Boolean>> call(
                List<Long> trafficValueList,
                Optional<Tuple3<Long, AlertType, Boolean>> previousStepState) {

            AlertType previousAlert = previousStepState.isPresent() ? previousStepState
                    .get()._2() : AlertType.NOTHING;

            Long sumOfTraffic = 0l;
            for (Long trafficValue : trafficValueList) {
                sumOfTraffic += trafficValue;
            }

            AlertType resultType = previousAlert;
            boolean fireEvent = false;

            if (sumOfTraffic > trafficLimits.getMaxLimit()) {
                if (previousAlert == AlertType.LOW
                        || previousAlert == AlertType.LOW_FROM_HIGH) {
                    resultType = AlertType.HIGH_FROM_LOW;
                    fireEvent = true;
                } else if (previousAlert != AlertType.HIGH
                        && previousAlert != AlertType.HIGH_FROM_LOW) {
                    resultType = AlertType.HIGH;
                    fireEvent = true;
                }
            } else if (sumOfTraffic < trafficLimits.getMinLimit()) {
                if (previousAlert == AlertType.HIGH
                        || previousAlert == AlertType.HIGH_FROM_LOW) {
                    resultType = AlertType.LOW_FROM_HIGH;
                    fireEvent = true;
                } else if (previousAlert != AlertType.LOW
                        && previousAlert != AlertType.LOW_FROM_HIGH) {
                    resultType = AlertType.LOW;
                    fireEvent = true;
                }
            } else {
                if (previousAlert == AlertType.LOW
                        || previousAlert == AlertType.NOTHING
                        || previousAlert == AlertType.LOW_FROM_HIGH) {
                    resultType = AlertType.NORMAL_FROM_LOW;
                    fireEvent = true;
                }
                if (previousAlert == AlertType.HIGH
                        || previousAlert == AlertType.HIGH_FROM_LOW) {
                    resultType = AlertType.NORMAL_FROM_HIGH;
                    fireEvent = true;
                }
            }
            return Optional.of(new Tuple3<>(
                    sumOfTraffic, resultType, fireEvent));
        }
    }

    private static final class TrafficToPairMapper implements
            PairFunction<HostTraffic, String, Long> {
        @Override
        public Tuple2<String, Long> call(HostTraffic arg0) {

            return new Tuple2<>("fakeKey", new Long(
                    arg0.getTrafficAmount()));
        }
    }


}
