package com.apachestreaming.streaming;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQueryException;

import java.util.concurrent.TimeoutException;

public class SimpleStreamingApp {
    public static void main(String[] args) throws TimeoutException, StreamingQueryException {

        SparkSession spark = SparkSession.builder()
                .appName("Simple Streaming App")
                .master("local[*]")
                .getOrCreate();


        // Read streaming data from socket
        Dataset<Row> lines = spark.readStream()
                .format("socket")
                .option("host", "localhost")
                .option("port", 9999)
                .load();

        // Write stream to console
        lines.writeStream()
                .format("console")
                .outputMode("append")
                .start()
                .awaitTermination();
    }
}
