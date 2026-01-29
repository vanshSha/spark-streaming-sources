package com.apachestreaming.streaming;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQueryException;

import java.util.concurrent.TimeoutException;

// First execute (ncat -lk 9999) in terminal and write some message then run this project .
public class SimpleStreamingApp {
    public static void main(String[] args) throws TimeoutException, StreamingQueryException {

        SparkSession spark = SparkSession.builder()
                .appName("SimpleStreamingApp") // Application name
                .master("local[*]") // It uses all threads
                .getOrCreate(); // If exists (resue) otherwise create


        // Read streaming data from socket
        Dataset<Row> lines = spark
                .readStream() // Streaming Source Starts Here
                .format("socket") // Source Type
                .option("host", "localhost") // configuration
                .option("port", 9999)
                .load();
        /*
        DataFrame() - A DataFrame is a collection of rows and columns with a fixed schema, processed by Spark.
        Load() - Create DataFrame, Read from DataSource
         */

        // Write stream to console
        lines.writeStream()  // This method use for writing data
                .format("console")  // Source Type
                .outputMode("append")  // .outputMode() controls which rows are written in each micro-batch.
                // append : means write in a new row
                .start() // Starts The Spark Structured Streaming Query.
                .awaitTermination(); // keeps the Spark streaming application running.
    }
}
