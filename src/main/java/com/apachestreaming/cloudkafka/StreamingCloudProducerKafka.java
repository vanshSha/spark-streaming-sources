package com.apachestreaming.cloudkafka;

import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

// I want to use this then first use Environmental Variable
// KAFKA_API_KEY, KAFKA_SECRET_KEY

public class StreamingCloudProducerKafka {
    private static final String TOPIC = "hello";

    public static void main(String[] args) throws InterruptedException {

        Properties props = new Properties();

        // Bootstrap server
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                "pkc-921jm.us-east-2.aws.confluent.cloud:9092");


        // Serializers
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName());

        // Confluent Cloud security config
        props.put("security.protocol", "SASL_SSL");
        props.put("sasl.mechanism", "PLAIN");

        String apiKey = System.getenv("KAFKA_API_KEY");
        String apiSecret = System.getenv("KAFKA_API_SECRET");

        props.put("sasl.jaas.config",
                "org.apache.kafka.common.security.plain.PlainLoginModule required "
                        + "username=\"" + apiKey + "\" "
                        + "password=\"" + apiSecret + "\";");


        props.put(ProducerConfig.CLIENT_ID_CONFIG, "StreamingCloudKafka");
        // Reliability
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 5);
        // Performance tuning
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
        props.put(ProducerConfig.LINGER_MS_CONFIG, 5);
        // Compression
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "gzip");

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        try {
            for (int a = 0; a < 10; a++) {
                String key = "key-" + a;
                String value = String.format("{\"id\": %d, \"message\": \"Teri maa ki Hello %d\"}", a, a);

                ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC, key, value);

                producer.send(record, (metadata, exception) -> {
                    if (exception != null) {
                        exception.printStackTrace();
                    } else {
                        System.out.println(
                                "Sent -> topic=" + metadata.topic() +
                                        ", partition=" + metadata.partition() +
                                        ", offset=" + metadata.offset()
                        );
                    }
                });
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            producer.flush();
            producer.close();
        }

        System.out.println(
                props.get(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG)
        );


    }
}
