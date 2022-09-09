package com.aware.example.spark;

import java.io.IOException;

import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

public class WordCount {
	public static void main(String[] args) {

		String logFile = "C:\\Users\\2129931\\OneDrive - Cognizant\\Desktop\\QCAware\\sparkstarter\\src\\main\\resources\\spark_example.txt";
		SparkSession spark = SparkSession.builder().appName("Simple Application").master("local[2]").getOrCreate();

		Dataset<String> logData = spark.read().textFile(logFile);
		logData.printSchema();

		long numAs = logData.filter((FilterFunction<String>) s -> s.contains("a")).count();
		long numBs = logData.count();
		System.out.println("Lines with a: " + numAs + ", lines with b: " + numBs);

		try {
			System.in.read();
			spark.stop();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}