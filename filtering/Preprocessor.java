import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;

import java.sql.Timestamp;
import java.util.*;

public class Preprocessor {
    public static String generate_data(String type, String value, String status) {
        return "{\"" + type + "\":" + value + ",\"status\":" + status + "}";
    }

    public static void main(String[] args) {
        Boolean is_filter = true;
        Boolean is_sample = true;
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "preprocessor");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "128.199.105.69:9091");
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        final String[] raw_topics = {"raw-temperature", "raw-humidity"};
        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> input = builder.stream(Arrays.asList(raw_topics));

        // Assumption: producer sent key-value pairs
        ArrayList <String> valid_sensor_names = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            valid_sensor_names.add("temperature_" + Integer.toString(i));
            valid_sensor_names.add("humidity_" + Integer.toString(i));
        }
        //input.to("preprocessed-temperature");

        // Filter erroneous records
        KStream<String,String> valid_records = input
                // Key and value mustn't be null
                .filter((k, v) -> (k != null && v != null) || !is_filter)
                // Name of the sensor should be valid
                .filter((sensor_name, value) -> valid_sensor_names.contains(sensor_name) || !is_filter)
                // Value should be double-parsable
                .filter((sensor_name, value) -> {
                    try {
                        Double.parseDouble(value);
                        return true;
                    } catch (NumberFormatException e) {
                        return false;
                    }
                });

        Random r = new Random();
        Timestamp start = new Timestamp(System.currentTimeMillis());

        KStream<String, String> sample_records = valid_records
                // Random sample, 20% dropout
                .filter((k, v) -> r.nextDouble() < 0.8 || !is_sample)
                // Synchronous sample
                .filter((k, v) -> {
                    Timestamp now = new Timestamp(System.currentTimeMillis());
                    return (now.getTime() - start.getTime()) % 3000 < 1000 || !is_sample;
                });
        //sample_records.to("preprocessed-temperature");

        Map<String, KStream<String, String>> branch = sample_records.split()
                // Temperature within (10,60) is in a safe temperature range
                .branch((String sensor_name, String value) -> sensor_name.contains("temp") && Double.valueOf(value) > 10.0 && Double.valueOf(value) < 60.0,
                        Branched.withConsumer(ks -> ks.mapValues((value) -> generate_data("temp", value.toString(), "safe")).to("preprocessed-temperature")))
                // Humidity within (20,70) is in a safe humidity range
                .branch((String sensor_name, String value) -> sensor_name.contains("humid") && Double.valueOf(value) > 20.0 && Double.valueOf(value) < 70.0,
                        Branched.withConsumer(ks -> ks.mapValues((value) -> generate_data("humid", value.toString(), "safe")).to("preprocessed-humidity")))
                // Temperature lower than 10 or higher than 60 is unsafe
                .branch((String sensor_name, String value) -> sensor_name.contains("temp"),
                        Branched.withConsumer(ks -> ks.mapValues((value) -> generate_data("temp", value, "unsafe")).to("preprocessed-temperature")))
                // Humidity lower than 20 or higher than 70 is unsafe
                .branch((String sensor_name, String value) -> sensor_name.contains("humid"),
                        Branched.withConsumer(ks -> ks.mapValues((value) -> generate_data("humid", value, "unsafe")).to("preprocessed-humidity")))
                .noDefaultBranch();

        Topology topology = builder.build();
        KafkaStreams streams = new KafkaStreams(topology, config);
        streams.cleanUp();
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}