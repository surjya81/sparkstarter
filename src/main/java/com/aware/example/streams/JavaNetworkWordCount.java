package com.aware.example.streams;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import scala.Tuple2;

public class JavaNetworkWordCount {

	// http://ltin400172.cts.com:4040/jobs/ -- application UI
	public static void main(String[] args) throws InterruptedException {

		// Create a local StreamingContext with two working thread and batch interval of
		// 1 second
		SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount");
		JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(1));

		// Create a DStream that will connect to hostname:port, like local host:9999
		JavaReceiverInputDStream<String> lines = jssc.socketTextStream("localhost", 8080);

		// Split each line into words
		JavaDStream<String> words = lines.flatMap(x -> Arrays.asList(x.split(" ")).iterator());

		// Count each word in each batch
		JavaPairDStream<String, Integer> pairs = words.mapToPair(s -> new Tuple2<>(s, 1));
		JavaPairDStream<String, Integer> wordCounts = pairs.reduceByKey((i1, i2) -> i1 + i2);

		// Print the first ten elements of each RDD generated in this DStream to the
		// console
		wordCounts.print();

		jssc.start(); // Start the computation
		jssc.awaitTermination(); // Wait for the computation to terminate

	}

}
