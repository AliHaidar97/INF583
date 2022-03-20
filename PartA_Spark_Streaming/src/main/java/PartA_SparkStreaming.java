import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.TimeUnit;

import scala.Tuple2;
import twitter4j.Status;
import twitter4j.TwitterObjectFactory;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.twitter.TwitterUtils;
import org.apache.spark.util.AccumulatorV2;
import org.apache.spark.util.CollectionAccumulator;
import org.apache.spark.util.LongAccumulator;

import java.util.*;
import java.util.Properties;

public class PartA_SparkStreaming{

	
	public static Long getLargestIntegerUsingApacheStreaming() throws IOException, InterruptedException {

		SparkConf sparkConf = new SparkConf().setAppName("SparkApp").setMaster("local[*]");

		JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.seconds(1));

		JavaSparkContext jsc = new JavaSparkContext(jssc.ssc().sc());

		FileReader fr = new FileReader(new File("integers.txt"));
		BufferedReader br = new BufferedReader(fr);
		String line;
		int count = 0;

		LongAccumulator maxInt = jssc.ssc().sc().longAccumulator();

		ArrayList<String> batch = new ArrayList<String>();
		Queue<JavaRDD<String>> rdds = new LinkedList<>();

		while ((line = br.readLine()) != null) {
			count += 1;
			if (count == 100) {
				JavaRDD<String> rdd = jsc.parallelize(batch);
				rdds.add(rdd);
				batch = new ArrayList<String>();
				count = 0;
			}
			batch.add(line);
		}
		JavaRDD<String> rdd = jsc.parallelize(batch);
		rdds.add(rdd);

		JavaDStream<String> stream = jssc.queueStream(rdds, true);

		JavaDStream<String> largestInteger = stream.reduce((a, b) -> {
			if (Integer.parseInt(a) >= Integer.parseInt(b))
				return a;
			return b;
		});

		largestInteger.foreachRDD(a -> {
			maxInt.setValue(Math.max(Integer.parseInt(a.collect().get(0)), maxInt.value()));
		});
		largestInteger.print();
		jssc.start();
		TimeUnit.SECONDS.sleep(11);
		jssc.stop();
		return maxInt.value();

	}

	public static double getAverageValue() throws IOException, InterruptedException {
		SparkConf sparkConf = new SparkConf().setAppName("SparkApp").setMaster("local[*]");

		JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.seconds(1));

		JavaSparkContext jsc = new JavaSparkContext(jssc.ssc().sc());

		FileReader fr = new FileReader(new File("integers.txt"));
		BufferedReader br = new BufferedReader(fr);
		String line;
		LongAccumulator sumOfAllElements = jssc.ssc().sc().longAccumulator();
		LongAccumulator countOfAllElements = jssc.ssc().sc().longAccumulator();
		int count = 0;
		ArrayList<String> batch = new ArrayList<String>();
		Queue<JavaRDD<String>> rdds = new LinkedList<>();
		while ((line = br.readLine()) != null) {
			count += 1;
			if (count == 100) {
				JavaRDD<String> rdd = jsc.parallelize(batch);
				rdds.add(rdd);
				batch = new ArrayList<String>();
				count = 0;
			}
			batch.add(line);
		}
		JavaRDD<String> rdd = jsc.parallelize(batch);
		rdds.add(rdd);

		JavaDStream<String> stream = jssc.queueStream(rdds, true);

		JavaPairDStream<Integer, Tuple2<Integer, Integer>> numbers2 = stream.mapToPair(s -> {
			return new Tuple2<>(1, new Tuple2(Integer.parseInt(s), 1));
		});

		JavaPairDStream<Integer, Tuple2<Integer, Integer>> sumOfnumbers = numbers2
				.reduceByKey((a, b) -> new Tuple2<>(a._1 + b._1, a._2 + b._2));

		JavaDStream<Double> average = (sumOfnumbers.mapValues(a -> {
			sumOfAllElements.add(a._1);
			countOfAllElements.add(a._2);
			return new Double((a._1 * 1.0) / a._2);
		}).map(a -> {
			return a._2;
		}));

		average.print();

		jssc.start();

		TimeUnit.SECONDS.sleep(11);
		jssc.stop();
		return (double) ((sumOfAllElements.value() * 1.0) / (countOfAllElements.value()));
	}

	public static int hash(int x) {
		return (3 * x + 5) % 128;
	}

	public static int leastSignificantBit(int x) {
		int i = 0;
		if (x == 0)
			return 6;
		while (x % 2 == 0) {
			x = x / 2;
			i++;
		}

		return i;
	}
	
	

	public static int getNumberOfDistinctInteger() throws IOException, InterruptedException {

		SparkConf sparkConf = new SparkConf().setAppName("SparkApp").setMaster("local[*]");

		JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.seconds(1));

		JavaSparkContext jsc = new JavaSparkContext(jssc.ssc().sc());


		LongAccumulator b0 = jssc.ssc().sc().longAccumulator();
		LongAccumulator b1 = jssc.ssc().sc().longAccumulator();
		LongAccumulator b2 = jssc.ssc().sc().longAccumulator();
		LongAccumulator b3 = jssc.ssc().sc().longAccumulator();
		LongAccumulator b4 = jssc.ssc().sc().longAccumulator();
		LongAccumulator b5 = jssc.ssc().sc().longAccumulator();
		LongAccumulator b6 = jssc.ssc().sc().longAccumulator();
		

		FileReader fr = new FileReader(new File("integers.txt"));
		BufferedReader br = new BufferedReader(fr);
		String line;

		int count = 0;
		ArrayList<String> batch = new ArrayList<String>();
		Queue<JavaRDD<String>> rdds = new LinkedList<>();
		while ((line = br.readLine()) != null) {
			count += 1;
			if (count == 100) {
				JavaRDD<String> rdd = jsc.parallelize(batch);
				rdds.add(rdd);
				batch = new ArrayList<String>();
				count = 0;
			}
			batch.add(line);
		}
		JavaRDD<String> rdd = jsc.parallelize(batch);
		rdds.add(rdd);

		JavaDStream<String> stream = jssc.queueStream(rdds, true);
		
		JavaPairDStream<Integer,Integer>bitRdd = stream.mapToPair(a->{
			return new Tuple2<>(leastSignificantBit(hash(Integer.parseInt(a))),1);}).reduceByKey((a,b)->(a+b));
		
		bitRdd.foreachRDD(a->{
			a.foreach(b->{
				if(b._1==0)
					b0.add(b._2);
				if(b._1==1)
					b1.add(b._2);
				if(b._1==2)
					b2.add(b._2);
				if(b._1==3)
					b3.add(b._2);
				if(b._1==4)
					b4.add(b._2);
				if(b._1==5)
					b5.add(b._2);
				if(b._1==6)
					b6.add(b._2);
				
			;});});
		bitRdd.print();

		jssc.start();
		
		TimeUnit.SECONDS.sleep(11);
		jssc.stop();
		long m = 0;
		if(b0.value()==0)m=0;
		else if(b1.value()==0)m=1;
		else if(b2.value()==0)m=2;
		else if(b3.value()==0)m=3;
		else if(b4.value()==0)m=4;
		else if(b5.value()==0)m=5;
		else if(b6.value()==0)m=6;
		else m=7;
		
		return (int) (Math.pow(2, m) / 0.73351);

	}

	public static void main(String[] args) throws ClassNotFoundException, IOException, InterruptedException {

		 System.out.println("LargestInterger is = "+ getLargestIntegerUsingApacheStreaming());
		// System.out.println("average is = "+ getAverageValue() );
		//System.out.println("numberOfDistinct value is = " + getNumberOfDistinctInteger());
	}
}