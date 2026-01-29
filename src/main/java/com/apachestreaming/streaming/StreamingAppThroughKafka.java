package com.apachestreaming.streaming;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQueryException;

import java.util.concurrent.TimeoutException;

public class StreamingAppThroughKafka {
    public static void main(String[] args) throws StreamingQueryException, TimeoutException {

        SparkSession sparkSession = SparkSession.builder()
                .appName("Simple Streaming App")
                .master("local[*]")
                .getOrCreate();

        Dataset<Row> Kafka = sparkSession.readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", "localhost:9092")
                .option("subscribe", "first-attempt")
                .option("startingOffsets", "earliest")
                .load();

        /*  subscribe : Subscribe to one topic
            subscribePattern : Subscribe to multiple topics  */

        Dataset<Row> messages = Kafka.selectExpr("CAST(value AS STRING) as message");

        messages.writeStream()
                .format("console")
                .outputMode("append")
                .option("checkpointLocation", "/tmp/kafka-checkpoint")
                /*
                checkpointLocation : Where Spark saves its progress so it can continue after a crash.
                /tmp/kafka-checkpoint : It is a folder path where spark save progress file
                 */
                .start()
                .awaitTermination();

    }
}
