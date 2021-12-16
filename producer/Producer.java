package com.nguyenthienan.phantom.producer;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

public class Producer {

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        String boostrapServers = "128.199.105.69:9093";

        Logger logger = LoggerFactory.getLogger(Producer.class);

        // create Producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, boostrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // create the producers
        KafkaProducer<String, String>[] producer = new KafkaProducer[20];

        Timer []time = new Timer[20];
        for (int i = 0; i < 20; i++) {
            producer[i] = new KafkaProducer<String, String>(properties);
            long random_start_time = (long) (Math.random()*10);
            System.out.println(random_start_time);
            if (i < 10) {
                time[i] = new Timer();
                time[i].schedule(new Publish("raw-temperature", String.valueOf(i), producer[i]), random_start_time, TimeUnit.SECONDS.toMillis(1));
            } else {
                time[i] = new Timer();
                time[i].schedule(new Publish("raw-humidity", String.valueOf(i - 10), producer[i]), random_start_time, TimeUnit.SECONDS.toMillis(1));
            }
        }
    }
}

class Publish extends TimerTask {
    String topic;
    String id;
    KafkaProducer<String, String> producer;
    public Publish (String Topic, String Id, KafkaProducer<String, String> Producer) {
        topic = Topic;
        id = Id;
        producer = Producer;
    }

    public void run() {
        try {
            String topic_name = topic.trim().split("-")[1];
            String value = "";
            double rnd = Math.random();
            if (rnd > 0.8) {
                value = "Garbage";
            } else if (topic_name.equals("temperature")) {
                value = String.valueOf(10 + Math.random() * 60);
            } else if (topic_name.equals("humidity")) {
                value = String.valueOf(20 + Math.random() * 60);
            }

            ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, topic_name + "_" + id, value);
            producer.send(record);
            producer.flush();
        } catch (Exception ex) {
            System.out.println("error running thread " + ex.getMessage());
        }
    }
}