package com.github.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;

import java.sql.Timestamp;
import java.util.*;

public class Preprocessor {
    public static String generate_data(String type, String value, String status) {
        return "{\"" + type + "\":" + value + ",\"status\":" + status;
    }

    public static void main(String[] args) {
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "favorite-colour");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        final String[] raw_topics = {"raw-temperature", "raw-humidity"};
        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> input = builder.stream(Arrays.asList(raw_topics));

        // Assumption: producer sent key-value pairs
        ArrayList <String> valid_sensor_names = new ArrayList<>();
        for (int i = 1; i <= 10; i++) {
            valid_sensor_names.add("temp_" + Integer.toString(i));
            valid_sensor_names.add("humid_" + Integer.toString(i));
        }

        // Filter erroneous records
        KStream<String,String> valid_records = input
                // Key and value mustn't be null
                .filter((k, v) -> k == null || v == null)
                // Name of the sensor should be valid
                .filter((sensor_name, value) -> Arrays.asList(valid_sensor_names).contains(sensor_name))
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
                .filter((k, v) -> r.nextDouble() > 0.8)
                // Synchronous sample
                .filter((k, v) -> {
                    Timestamp now = new Timestamp(System.currentTimeMillis());
                    return (now.getTime() - start.getTime()) % 5000 < 100;
                });

        Map<String, KStream<String, String> > branch = sample_records.split()
                .branch((String sensor_name, String value) -> sensor_name.contains("temp") && Double.valueOf(value) > 10.0 && Double.valueOf(value) < 60.0, Branched.as("safe-temp"))
                .branch((String sensor_name, String value) -> sensor_name.contains("humid") && Double.valueOf(value) > 20.0 && Double.valueOf(value) < 70.0, Branched.as("safe-humid"))
                .branch((String sensor_name, String value) -> sensor_name.contains("temp"), Branched.as("unsafe-temp"))
                .branch((String sensor_name, String value) -> sensor_name.contains("humid"), Branched.as("unsafe-humid"))
                .noDefaultBranch();

        branch.get("safe-temp").mapValues((value) -> generate_data("temp", value, "safe")).to("preprocessed-temperature");
        branch.get("safe-humid").mapValues((value) -> generate_data("humid", value, "safe")).to("preprocessed-humidity");
        branch.get("unsafe-temp").mapValues((value) -> generate_data("temp", value, "unsafe")).to("preprocessed-temperature");
        branch.get("unsafe-humid").mapValues((value) -> generate_data("humid", value, "unsafe")).to("preprocessed-humidity");

        Topology topology = builder.build();
        KafkaStreams streams = new KafkaStreams(topology, config);
        streams.cleanUp();
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}
