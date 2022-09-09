package com.aware.example.streams;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import scala.Tuple2;

public final class JavaQueueStream {
	private JavaQueueStream() {
	}

	public static void main(String[] args) throws Exception {

//		StreamingExamples.setStreamingLogLevels();
		SparkConf sparkConf = new SparkConf().setAppName("JavaQueueStream").setMaster("local[2]");

		// Create the context
		JavaStreamingContext ssc = new JavaStreamingContext(sparkConf, new Duration(1000));

		// Create the queue through which RDDs can be pushed to
		// a QueueInputDStream (TODO)

		// Create and push some RDDs into the queue
		List<Integer> list = new ArrayList<>();
		for (int i = 0; i < 1000; i++) {
			list.add(i);
		}
		System.out.println(list);
		Queue<JavaRDD<Integer>> rddQueue = new LinkedList<>();
		for (int i = 0; i < 30; i++) {
			rddQueue.peek();
			rddQueue.add(ssc.sparkContext().parallelize(list));
		}
		rddQueue.spliterator().forEachRemaining(x -> x.getResourceProfile());
		// Create the QueueInputDStream and use it do some processing
		JavaDStream<Integer> inputStream = ssc.queueStream(rddQueue);
		JavaPairDStream<Integer, Integer> mappedStream = inputStream.mapToPair(i -> new Tuple2<>(i % 10, 1));
		JavaPairDStream<Integer, Integer> reducedStream = mappedStream.reduceByKey((i1, i2) -> i1 + i2);

		reducedStream.print();
		ssc.start();
		ssc.close();
	}
}
