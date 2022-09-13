package com.aware.example.sql;

import static org.apache.spark.sql.functions.callUDF;
import static org.apache.spark.sql.functions.col;

import java.io.IOException;

import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DataTypes;

public class SparkDatasetTest {

	private static final String CSV_URL = "C:\\Users\\2129931\\OneDrive - Cognizant\\Desktop\\QCAware\\sparkstarter\\src\\main\\resources\\username.csv";
	private static final String ACTUAL_CATEGORY_FUNCTION = "getActualCategoryType";

	public static void main(String[] args) throws InterruptedException {
		SparkSession spark = SparkSession.builder().appName("SparkdatasetTest").master("local[*]").getOrCreate();
		Dataset<Row> csv = spark.read().format("csv").option("sep", ",").option("inferSchema", "true")
				.option("header", "true").load(CSV_URL);

		csv.show();
		csv.printSchema();

		// filter the rows from DataFrame or Dataset based on the given one or multiple
		// conditions or SQL expressions.
		csv.filter("Identifier >5000").show();
		// similar as above using functions
		csv.filter(new FilterFunction<Row>() {
			@Override
			public boolean call(Row row) throws Exception {
				return row.getAs("Last name").equals("Johnson");
			}
		}).show();
		// Custom user defined function
		spark.udf().register(ACTUAL_CATEGORY_FUNCTION, getAccurateCategoryType, DataTypes.StringType);
		csv.withColumn("Category", callUDF(ACTUAL_CATEGORY_FUNCTION, col("Category"))).show();
		// Aggregation
		csv.groupBy(col("Category")).sum("Identifier").show();
		try {
			System.in.read();
			spark.stop();
		} catch (IOException e) {
			e.printStackTrace();
		}
	} // End of Main

	private static final UDF1 getAccurateCategoryType = new UDF1<Integer, String>() {
		@Override
		public String call(final Integer i) throws Exception {
			switch (i) {
			case 1:
				return "CATAGORY 1";
			case 2:
				return "CATAGORY 2";
			case 3:
				return "CATAGORY 3";
			}
			return null;
		}
	};
}
