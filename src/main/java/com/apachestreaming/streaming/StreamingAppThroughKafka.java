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

        Dataset<Row> kafkaDf = sparkSession.readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", "localhost:9092")
                .option("subscribe", "first-attempt")
                .option("startingOffsets", "earliest")
                .load();

        Dataset<Row> messages = kafkaDf.selectExpr(
                "CAST(value AS STRING) as message"
        );

        messages.writeStream()
                .format("console")
                .outputMode("append")
                .option("checkpointLocation", "/tmp/kafka-checkpoint")
                .start()
                .awaitTermination();

    }
}
