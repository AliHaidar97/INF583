package inf583.wordcount_spark;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.io.IOException;

import java.util.StringTokenizer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.*;

import scala.Tuple2;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapred.lib.IdentityMapper;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.join.TupleWritable;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;



public class Part_A_Using_Spark 
{

	static int getLargestIntegerUsingSpark(String inputFile,String outputFolder) {

		// Create a Java Spark Context
		SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("wordCount");
		JavaSparkContext sc = new JavaSparkContext(conf);

		// Load our input data.
		JavaRDD<String> input = sc.textFile(inputFile);


		// A1 Finding the largest integer
		// Map phase: for each number, we create a pair (1, number)
		// Reduce phase: for each pair of tuples (1,a) , (1,b) we reduce them to (1, max(a,b)) 

		JavaPairRDD<Integer, Integer> numbers1 = input.mapToPair(s ->
		{ String line = s; return new Tuple2<>(1, Integer.parseInt(line));});


		JavaPairRDD<Integer, Integer> maxInt = numbers1.reduceByKey((a,b) -> Math.max(a,b));


		for (Tuple2<Integer, Integer> val :maxInt.collect()) {
			System.out.println("The max  is : " + val._2);
		}    	
		System.out.println("Finish");

		return maxInt.collect().get(0)._2;
	}


	static double getAverageOfAllIntegersUsingSpark(String inputFile,String outputFolder) {

		SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("wordCount");
		JavaSparkContext sc = new JavaSparkContext(conf);

		// Load our input data.
		JavaRDD<String> input = sc.textFile(inputFile);


		// A2 Finding the average of all integers
		// Map phase: for each number, we create a pair(1, pair(number, 1))
		// Reduce phase: for each pair of tuples (1,(a1,b1)) , (1,(a2,b2)) we reduce them to (1,(a1+a2, b1+b2))

		JavaPairRDD<Integer, Tuple2<Integer, Integer>> numbers2 = input.mapToPair(s ->
		{ String line = s; return new Tuple2<>(1, new Tuple2(Integer.parseInt(line),1));});

		JavaPairRDD<Integer, Tuple2<Integer, Integer>> sumOfnumbers = numbers2.reduceByKey((a,b) -> new Tuple2<>(a._1+b._1,a._2+b._2));

		JavaPairRDD<Integer, Double> average = sumOfnumbers.mapValues( a -> new Double((a._1*1.0)/a._2));

		/*
    	List<Tuple2<Integer,Double>> test  = average.collect();
    	System.out.println(test.get(0)._2());
		 */
		for (Tuple2<Integer, Double> val :average.collect()) {
			System.out.println("The average  is : " + val._2);
		}    	
		System.out.println("Finish");

		return average.collect().get(0)._2;
	}

	static void distinctFilterUsingSpark(String inputFile,String outputFolder) {

		SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("wordCount");
		JavaSparkContext sc = new JavaSparkContext(conf);

		// Load our input data.
		JavaRDD<String> input = sc.textFile(inputFile);

		// A3 Printing unique elements
		// Map phase: for each number, we create a pair(number, 0)
		// Reduce phase: for each pair of tuples (a,0) , (a,0) we reduce them to (a,0)

		JavaPairRDD<Integer, Integer> numbers3 = input.mapToPair(s ->
		{ String line = s; return new Tuple2<>(Integer.parseInt(line),0);});


		JavaPairRDD<Integer, Integer> sumByKey = numbers3.reduceByKey((a,b) -> 0);

		System.out.println("The values are : ");
		for (Tuple2<Integer, Integer> val : sumByKey.collect()) {
			System.out.println(val._1);
		}    	
		System.out.println("Finish");

	}


	static int getNumberOfDistinctInteger(String inputFile,String outputFolder) {

		SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("wordCount");
		JavaSparkContext sc = new JavaSparkContext(conf);

		// Load our input data.
		JavaRDD<String> input = sc.textFile(inputFile);

		// A4 Printing the number of unique elements
		// Map phase: for each number, we create a pair(number, 0) 
		// Reduce phase: for each pair of tuples (a,0) , (a,0) we reduce them to (a,0)
		// Map phase: we map each (number,0) to (1,1)
		// Reduce phase: for each pair of tuples (1,a),(1,b) we reduce them to (1,a+b)

		JavaPairRDD<Integer, Integer> numbers4 = input.mapToPair(s ->
		{ String line = s; return new Tuple2<>(Integer.parseInt(line),0);});


		JavaPairRDD<Integer, Integer> numberOfDifferentElem = numbers4.reduceByKey((a,b) -> 0).mapToPair(s ->{ Tuple2<Integer, Integer> elem = s; return new Tuple2<>(1,1);}).reduceByKey((a,b)->a+b);


		for (Tuple2<Integer, Integer> val : numberOfDifferentElem.collect()) {
			System.out.println("Number of different elements : " + val._2);
		}    	
		System.out.println("Finish");

		return numberOfDifferentElem.collect().get(0)._2;

	}
	public static void main(String args[]) {
		//getLargestIntegerUsingSpark("integers.txt","output");
		//getAverageOfAllIntegersUsingSpark("integers.txt","output");
		//distinctFilterUsingSpark("integers.txt","output");
		getNumberOfDistinctInteger("integers.txt","output");
	}
}
