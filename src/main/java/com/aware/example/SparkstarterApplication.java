package com.aware.example;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class SparkstarterApplication {

	public static void main(String[] args) throws InterruptedException {
		SpringApplication.run(SparkstarterApplication.class, args);
		System.out.println("----------------------Start Apache Spark--------------------");
	}

}
