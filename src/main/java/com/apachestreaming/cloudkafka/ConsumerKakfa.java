package com.apachestreaming.cloudkafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class ConsumerKakfa {

    public static void main(String[] args) {

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                "pkc-921jm.us-east-2.aws.confluent.cloud:9092");

        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());

        props.put("security.protocol", "SASL_SSL");
        props.put("sasl.mechanism", "PLAIN");

        String apiKey = System.getenv("KAFKA_API_KEY");
        String apiSecret = System.getenv("KAFKA_API_SECRET");

        props.put("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required " + "username=\"" + apiKey + "\" " +
                        "password=\"" + apiSecret + "\";");

        props.put(ConsumerConfig.CLIENT_ID_CONFIG, "consumer-client-1");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "consumer-group-v1");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

        KafkaConsumer<String, String> consumer =
                new KafkaConsumer<>(props);

        String TOPIC = "hello";
        consumer.subscribe(Collections.singletonList(TOPIC));
        System.out.println("Subscribed to topic: " + TOPIC);

        try {
            while (true) {
                ConsumerRecords<String, String> records =
                        consumer.poll(Duration.ofSeconds(1));

                if (records.isEmpty()) {
                    continue; // same as msg is None
                }

                for (ConsumerRecord<String, String> record : records) {
                    // Process message
                    System.out.println("Received message: key=" + record.key() + ", value=" + record.value() + ", partition=" + record.partition() +
                                    ", offset=" + record.offset());}
                // Commit offsets AFTER processing
                consumer.commitSync();}
        } catch (WakeupException e) {
            // Ignore if closing
            System.out.println("Consumer interrupted");
        } catch (Exception e) {
            System.out.println("Kafka error occurred: " + e.getMessage());
        } finally {
            consumer.close();
            System.out.println("Consumer closed");
        }
    }
}
