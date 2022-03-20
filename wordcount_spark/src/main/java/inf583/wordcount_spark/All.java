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


/**
 * Hello world!
 *
 */
public class All 
{

	int getLargestIntegerUsingSpark(String inputFile,String outputFolder) {

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


	double getAverageOfAllIntegersUsingSpark(String inputFile,String outputFolder) {

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

	void distinctFilterUsingSpark(String inputFile,String outputFolder) {

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


	int getNumberOfDistinctInteger(String inputFile,String outputFolder) {

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


	static int getMostImportantPageUsingSparkUsingArray(String inputFile, String outputFolder, int numberOfIteration) {

		// Create a Java Spark Context
		SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("wordCount");
		JavaSparkContext sc = new JavaSparkContext(conf);

		// Load our input data.
		JavaRDD<String> input = sc.textFile(inputFile);

		int numberOfNodes = (input.mapToPair(a-> {return new Tuple2<>(1,Integer.parseInt(a.split(" ")[0]));})).reduceByKey((a,b)->Math.max(a,b)).collect().get(0)._2 + 1;

		System.out.println("numberOfNodes = " + numberOfNodes);

		JavaRDD<String> edges = input.flatMap(s ->
		{String[] nodes = s.split(" "); for(int i=1;i<nodes.length;i++) nodes[i]=nodes[0]+" "+nodes[i]; return  Arrays.asList(Arrays.copyOfRange(nodes, 1, nodes.length)).iterator();});


		JavaPairRDD<Integer, Tuple2<Integer,Double>> graph = edges.mapToPair(s ->
		{ String[] edge = s.split(" ") ; return new Tuple2<>(Integer.parseInt(edge[0]), new Tuple2<>(Integer.parseInt(edge[1]),1.0));});

		double[] arr = new double [numberOfNodes];

		for(int i=0; i < numberOfNodes; i++ ) {
			arr[i] = 1.0/numberOfNodes;

		}    	

		JavaPairRDD<Integer,Double> r ;

		for(int i = 0; i<numberOfIteration;i++) {



			r = (graph.mapValues(a -> (new Tuple2<>(a._1,arr[a._1]))).reduceByKey((a,b) -> new Tuple2<>(a._1,(a._2 + b._2)))).mapValues(a -> new Double(a._2));

			double norm =0;
			norm  = r.mapToPair(a->{return new Tuple2<>(1,a._2*a._2);}).reduceByKey((a,b)->a+b).lookup(1).get(0);

			for(int j=0;j<numberOfNodes;j++) {
				arr[j]=0;
			}


			for (Tuple2<Integer, Double> val : r.collect()) {

				arr[val._1] = val._2/norm;

			}   

		}
		int bestNode = 0;
		for(int i=0;i<numberOfNodes;i++) {
			if(arr[bestNode] < arr[i]) {
				bestNode = i;
			}
		}
		return bestNode;

	}



	static int getMostImportantPageUsingSparkUsingOnlyRDD(String inputMatrixFile,String inputRFile, String outputFolder, int numberOfIteration) {

		// Create a Java Spark Context
		SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("wordCount");
		JavaSparkContext sc = new JavaSparkContext(conf);

		// Load our input data.
		JavaRDD<String> inputMatrix = sc.textFile(inputMatrixFile);
		JavaRDD<String> inputR = sc.textFile(inputRFile);
		long numberOfNodes =  (inputR.count());

		System.out.println("numberOfNodes = " + numberOfNodes);

		JavaRDD<String> edges = inputMatrix.flatMap(s ->
		{String[] nodes = s.split(" "); for(int i=1;i<nodes.length;i++) nodes[i]=nodes[0]+" "+nodes[i]; return  Arrays.asList(Arrays.copyOfRange(nodes, 1, nodes.length)).iterator();});


		JavaPairRDD<String,String> graph = edges.mapToPair(s ->
		{ String[] edge = s.split(" ") ; return new Tuple2<>(edge[0]+"#"+edge[1], "M#1.0");});

		JavaRDD<String> r = inputR;

		for(int i = 0; i<numberOfIteration;i++) {

			JavaPairRDD<String,String> rTransform = r.flatMap(s->{
				ArrayList<String> temp = new ArrayList<>();
				for(int j=0;j<2;j++) {
					temp.add(j+"#"+s.split(" ")[0] + " "  + "V#"+s.split(" ")[1]);
				}
				return temp.iterator();
			}).mapToPair(s->{
				return new Tuple2<>(s.split(" ")[0],s.split(" ")[1]);
			});

			rTransform.saveAsTextFile("outputR");
			/*
			JavaPairRDD<String,String> tempR= graph.union(rTransform).reduceByKey((a,b) -> {
				double result = Double.parseDouble(a.split("#")[1])*Double.parseDouble(b.split("#")[1]);
				return "M#result";
			}).mapToPair(s-> 
			{
				if(s._2.split("#")[0]=="M")
				return new Tuple2<>(s._1.split("#")[0], s._2.split("#")[1]);
				return new Tuple2<>(s._1.split("#")[0],"0.0");
			}
			).reduceByKey((a,b)->{return Double.toString(Double.parseDouble(a)+Double.parseDouble(b));});


			double norm = tempR.mapToPair(a-> {return new Tuple2<>(1,Math.pow(Double.parseDouble(a._2),2));}).reduceByKey((a,b) -> {return (a+b);}).collect().get(0)._2;
			tempR = tempR.mapValues(a-> Double.toString(Double.parseDouble(a)/norm));			

			r = tempR.map(s->(s._1+" "+s._2));
			 */
		}

		String bestNode = r.mapToPair(s->{return new Tuple2<>(1,s);}).reduceByKey((a,b)->{
			if(Double.parseDouble(a.split(" ")[1]) > Double.parseDouble(b.split(" ")[1])) {
				return a;
			}
			return b;}
				).collect().get(0)._2;


		return Integer.parseInt(bestNode.split(" ")[0]);

	}


	public static class MapperExerciseB2
	extends Mapper<Object, Text, IntWritable, DoubleWritable>{

		private  static IntWritable firstNode = new IntWritable(1);
		private static DoubleWritable secondNode = new DoubleWritable();

		public void map(Object key, Text value, Context context
				) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			String arr[] = conf.getStrings("arr");
			String[] s = (value.toString()).split(" ");

			for(int i=1;i<s.length;i++) {
				firstNode.set(Integer.parseInt(s[0]));
				secondNode.set(Double.parseDouble(arr[(Integer.parseInt(s[i]))]));
				context.write(firstNode, secondNode);
			}	
		}
	}


	public static class ReducerExerciseB2
	extends Reducer<IntWritable,DoubleWritable,IntWritable,DoubleWritable> {
		private DoubleWritable result = new DoubleWritable();

		public void reduce(IntWritable key, Iterable<DoubleWritable> values,
				Context context
				) throws IOException, InterruptedException {

			double  sum = 0;

			for (DoubleWritable val : values) {
				sum+=val.get();
			}

			result.set(sum);
			context.write(key, result); 
		}

	}


	static int getMostImportantPageUsingHadoopUsingArray(String inputFile, String outputFolder, int numberOfIteration) throws IOException, ClassNotFoundException, InterruptedException {

		ArrayList<String> list = new ArrayList<String>();
		for(int i=0;i<64374;i++) {
			list.add(Double.toString(1.0/64374));
		}
		String[] arr = list.toArray(new String[list.size()]);
		for(int i=0;i<numberOfIteration;i++) {
			Configuration conf = new Configuration();
			conf.setStrings("arr", arr);
			Job job1 = Job.getInstance(conf, "part-B2");
			job1.setJarByClass(All.class);
			job1.setMapperClass(MapperExerciseB2.class);
			job1.setCombinerClass(ReducerExerciseB2.class);
			job1.setReducerClass(ReducerExerciseB2.class);
			job1.setOutputKeyClass(IntWritable.class);
			job1.setOutputValueClass(DoubleWritable.class);
			FileInputFormat.addInputPath(job1, new Path(inputFile));
			FileOutputFormat.setOutputPath(job1, new Path(outputFolder));
			job1.waitForCompletion(true);

		}
		return 0;

	}


	public static class MapperExerciceMatrixWithoutArrayB2_1
	extends Mapper<Object, Text, Text, Text>{

		private  static Text firstNode = new Text();
		private static Text secondNode = new Text();

		public void map(Object key, Text value, Context context
				) throws IOException, InterruptedException {

			String[] list = value.toString().split(" ");
			
			for(int i=1;i<list.length;i++) {
				firstNode.set(list[i]);
				secondNode.set("M#"+list[0]+"#"+"1");
				context.write(firstNode, secondNode); 
				
			}
		}
	}


	public static class MapperExerciceVectorWithoutArrayB2_1
	extends Mapper<Object, Text, Text, Text>{

		private  static Text firstNode = new Text();
		private static Text secondNode = new Text();

		public void map(Object key, Text value, Context context
				) throws IOException, InterruptedException {

			String[] list = value.toString().split("	");
			firstNode.set(list[0]);
			secondNode.set("A#"+list[0]+"#"+list[1]);
			
			context.write(firstNode,secondNode);
			
		}
	}

	public static class ReducerExerciseWithoutArrayB2_1
	extends Reducer<Text,Text,Text,Text> {

		private  static Text firstNode = new Text();
		private static Text secondNode = new Text();
		public void reduce(Text key, Iterable<Text> values,
				Context context
				) throws IOException, InterruptedException {

			ArrayList<String> list = new ArrayList<>();
			
			for(Text val:values) {
				list.add(val.toString());
			}
			
			Collections.sort(list);   
			
		
			String val = list.get(0).split("#")[2];
			
			
			firstNode.set(list.get(0).split("#")[1]);
			
			secondNode.set("0");
			
			
			context.write(firstNode, secondNode);
			
			
			for(int i=1;i<list.size();i++) {

				firstNode.set(list.get(i).split("#")[1]);
				secondNode.set(val);
				
				context.write(firstNode, secondNode);

			}
			

		}

	}



	public static class MapperExerciceWithoutArrayB2_2
	extends Mapper<Object, Text, Text, Text>{

		private  static Text firstNode = new Text();
		private static Text secondNode = new Text();

		public void map(Object key, Text value, Context context
				) throws IOException, InterruptedException {

			String[] list = value.toString().split("\t");
			
			firstNode.set(list[0]);
			secondNode.set(list[1]);
			context.write(firstNode,secondNode);
		
		}
	}

	public static class ReducerExerciseWithoutArrayB2_2
	extends Reducer<Text,Text,Text,Text> {

		private  static Text firstNode = new Text();
		private static Text secondNode = new Text();
		public void reduce(Text key, Iterable<Text> values,
				Context context
				) throws IOException, InterruptedException {

			double result = 0.0;
			
			for(Text val:values) {
				result+= Double.parseDouble(val.toString());
			}
			
			secondNode.set(Double.toString(result));
			firstNode.set(key);
			
			context.write(firstNode, secondNode);

		}

	}

	
	static int getMostImportantPageUsingHadoop(String inputFile, String inputRFile, String output, int numberOfIteration) throws IOException, ClassNotFoundException, InterruptedException {

		for(int i=0;i<numberOfIteration;i++) {
			Configuration conf = new Configuration();
			Job job1 = Job.getInstance(conf, "part-B2");
			job1.setJarByClass(All.class);
			job1.setReducerClass(ReducerExerciseWithoutArrayB2_1.class);
			job1.setOutputKeyClass(Text.class);
			job1.setOutputValueClass(Text.class);
			
			job1.setInputFormatClass(TextInputFormat.class);
	        job1.setOutputFormatClass(TextOutputFormat.class);
	        
	        if(i==0)
	        	MultipleInputs.addInputPath(job1, new Path(inputRFile),  TextInputFormat.class, MapperExerciceVectorWithoutArrayB2_1.class);
	        else 
	        	MultipleInputs.addInputPath(job1, new Path(output),  TextInputFormat.class, MapperExerciceVectorWithoutArrayB2_1.class);
			
	        
			MultipleInputs.addInputPath(job1, new Path(inputFile),  TextInputFormat.class, MapperExerciceMatrixWithoutArrayB2_1.class);
			
			FileSystem fs = FileSystem.get(conf);
			if (fs.exists(new Path("outputIntermediate")))
	            fs.delete(new Path("outputIntermediate"), true);
			
			TextOutputFormat.setOutputPath(job1, new Path("outputIntermediate"));
			job1.waitForCompletion(true);

			
			job1 = Job.getInstance(conf, "part-B2");
			job1.setJarByClass(All.class);
			job1.setMapperClass(MapperExerciceWithoutArrayB2_2.class);
			job1.setReducerClass(ReducerExerciseWithoutArrayB2_2.class);
			job1.setOutputKeyClass(Text.class);
			job1.setOutputValueClass(Text.class);
			job1.setInputFormatClass(TextInputFormat.class);
	        job1.setOutputFormatClass(TextOutputFormat.class);
	        
	        TextInputFormat.addInputPath(job1, new Path("outputIntermediate"));
			if (fs.exists(new Path(output)))
	            fs.delete(new Path(output), true);
			TextOutputFormat.setOutputPath(job1, new Path(output));
			job1.waitForCompletion(true);
			
			normalizeVectorUsingHadoop(output);
			

		}
		return 0;

	}




	public  static class MatrixMapperVector extends Mapper<Object, Text, Text, Text> 

	{

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException 

		{

			Configuration conf = context.getConfiguration(); 

			int m = Integer.parseInt(conf.get("m"));

			int p = Integer.parseInt(conf.get("p")); 
			
			

			String line = value.toString();

			String[] indicesAndValue = line.split("\t"); 

			Text outputKey = new Text();

			Text outputValue = new Text();

			

			
			for (int i = 0; i < m; i++) 

			{
				
				outputKey.set(i + "," + "0");
				
				outputValue.set("N," + indicesAndValue[0] + "," + indicesAndValue[1]); 
				
				
				context.write(outputKey, outputValue);

			}
			
			System.out.println(indicesAndValue[0]+" Done");
			

		}

	}



	public static class MatrixMapperMatrix extends Mapper<Object, Text, Text, Text> 

	{

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException 

		{

			Configuration conf = context.getConfiguration(); 

			int m = Integer.parseInt(conf.get("m"));

			int p = Integer.parseInt(conf.get("p")); 
			
			
			
			
			String line = value.toString();

			String[] indicesAndValue = line.split(" "); 

			Text outputKey = new Text();

			Text outputValue = new Text();




			for(int i=1;i<indicesAndValue.length;i++) {
				for (int k = 0; k < p; k++)
				{
					outputKey.set(indicesAndValue[0] + "," + k);

					outputValue.set("M," + indicesAndValue[i] + "," + "1"); 

					context.write(outputKey, outputValue);

				}
			}

		}

	}




	public static class MatrixReducer extends Reducer<Text, Text, Text, Text> 

	{

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException 

		{

			String[] value;

			HashMap<Integer, Float> hashA = new HashMap<Integer, Float>(); 

			HashMap<Integer, Float> hashB = new HashMap<Integer, Float>(); 

			for (Text val : values) 

			{

				value = val.toString().split(",");

				if (value[0].equals("M"))

				{

					hashA.put(Integer.parseInt(value[1]), Float.parseFloat(value[2]));

				} 

				else

				{

					hashB.put(Integer.parseInt(value[1]), Float.parseFloat(value[2]));

				}

			}

			int n = Integer.parseInt(context.getConfiguration().get("n")); float result = 0.0f;

			float a_ij; float b_jk;

			for (int j = 0; j < n; j++) 

			{

				a_ij = hashA.containsKey(j) ? hashA.get(j) : 0.0f; 

				b_jk = hashB.containsKey(j) ? hashB.get(j) : 0.0f; 

				result += a_ij * b_jk;

			}

			if (result != 0.0f) 

			{

				context.write(new Text(""), new Text(key.toString() + "\t" + Float.toString(result)));

			}

		}

	}




	static void matrixMultiplicationUsingOneMapReduce(String input,String inputR,String outputFolder) throws IOException, ClassNotFoundException, InterruptedException {

		Configuration conf = new Configuration();

		// M is an m-by-n matrix; N is an n-by-p matrix. 

		conf.set("m", "64375");

		conf.set("n", "64375");

		conf.set("p", "1");
		
		
		
		Job job1 = Job.getInstance(conf, "MatrixMultiplication");
		job1.setJarByClass(All.class);
		job1.setReducerClass(MatrixReducer.class);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(Text.class);
		
		job1.setInputFormatClass(TextInputFormat.class);
        job1.setOutputFormatClass(TextOutputFormat.class);
        
       
        MultipleInputs.addInputPath(job1, new Path(inputR),  TextInputFormat.class, MatrixMapperVector.class);

		
        MultipleInputs.addInputPath(job1, new Path(input),  TextInputFormat.class, MatrixMapperMatrix.class);
		
		FileSystem fs = FileSystem.get(conf);
		if (fs.exists(new Path(outputFolder)))
            fs.delete(new Path(outputFolder), true);
		
		TextOutputFormat.setOutputPath(job1, new Path(outputFolder));
		job1.waitForCompletion(true);
	}






	public static class MatrixMapperVector2_1 extends Mapper<Object, Text, Text, Text> 

	{

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException 

		{

			Configuration conf = context.getConfiguration(); 
			
		

			String line = value.toString();

			

			Text outputKey = new Text();

			Text outputValue = new Text();

			outputKey.set(line.split("\t")[0]);
			
			outputValue.set("N," + "0" + "," + line.split("\t")[1]); 
			
			
			context.write(outputKey, outputValue);

			
		}
	}


	public static class MatrixMapperMatrix2_1 extends Mapper<Object, Text, Text, Text> 

	{

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException 

		{

			Configuration conf = context.getConfiguration(); 


			String[] nodes = value.toString().split(" ");



			Text outputKey = new Text();

			Text outputValue = new Text();

			// i j j k 

			//M i j Mij
		
			for(int i=1;i<nodes.length;i++) {

				outputKey.set(nodes[i]);

				outputValue.set("M," + nodes[0] + "," + "1"); 

				context.write(outputKey, outputValue);
				
				
			} 

		}
	}


	public static class MatrixReducer2_1 extends Reducer<Text, Text, Text, Text> 

	{

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException 

		{

			String[] value;

			HashMap<Integer, Float> hashA = new HashMap<Integer, Float>(); 

			HashMap<Integer, Float> hashB = new HashMap<Integer, Float>(); 

			for (Text val : values) 

			{

				value = val.toString().split(",");
				
				if (value[0].equals("M"))

				{

					hashA.put(Integer.parseInt(value[1]), Float.parseFloat(value[2]));

				} 

				else

				{
				

					hashB.put(Integer.parseInt(value[1]), Float.parseFloat(value[2]));

				}

			}


			
			float a_ij; float b_jk;
			
			for(Entry<Integer, Float> b : hashB.entrySet()) {

			
				float result = 0;
				
				context.write(new Text(key.toString() + "," + Integer.toString(b.getKey())), new Text(Float.toString(result)));
				
			}
			

			for (Entry<Integer, Float> a : hashA.entrySet()) {

				for(Entry<Integer, Float> b : hashB.entrySet()) {

					a_ij = a.getValue(); 

					b_jk = b.getValue();
					
					

					float result = a_ij * b_jk;
					
					
					
					context.write(new Text(Integer.toString(a.getKey()) + "," + Integer.toString(b.getKey())), new Text(Float.toString(result)));
					
				}
			}

		}
	}

	public static class MatrixMapper2_2 extends Mapper<Object, Text, Text, Text> 

	{

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException 

		{

			Configuration conf = context.getConfiguration(); 


			context.write(new Text(value.toString().split("\t")[0]),new Text(value.toString().split("\t")[1]));

		}
	}


	public static class MatrixReducer2_2 extends Reducer<Text, Text, Text, Text> 

	{

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException 

		{

			String value;
			float result = (float) 0.0;
			for (Text val : values) 

			{

				value = val.toString();
				result += Float.parseFloat(value); 

			}

			context.write(new Text(key.toString().split(",")[0]), new Text(Float.toString(result)));

		}

	}


	static void matrixMultiplicationUsingTwoMapReduce(String input,String inputR,String outputFolder) throws IOException, ClassNotFoundException, InterruptedException {

		
		Configuration conf = new Configuration();

		// M is an m-by-n matrix; N is an n-by-p matrix. 

		conf.set("m", "64375");

		conf.set("n", "64375");

		conf.set("p", "1");
		
		
		
		Job job1 = Job.getInstance(conf, "MatrixMultiplication");
		job1.setJarByClass(All.class);
		job1.setReducerClass(MatrixReducer2_1.class);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(Text.class);
		
		job1.setInputFormatClass(TextInputFormat.class);
        job1.setOutputFormatClass(TextOutputFormat.class);
        
       
        MultipleInputs.addInputPath(job1, new Path(inputR),  TextInputFormat.class, MatrixMapperVector2_1.class);

		
        MultipleInputs.addInputPath(job1, new Path(input),  TextInputFormat.class, MatrixMapperMatrix2_1.class);
		
		FileSystem fs = FileSystem.get(conf);
		if (fs.exists(new Path("outputIntermediate")))
            fs.delete(new Path("outputIntermediate"), true);
		
		TextOutputFormat.setOutputPath(job1, new Path("outputIntermediate"));
		job1.waitForCompletion(true);
		
		
		conf = new Configuration();
		
		job1 = Job.getInstance(conf, "MatrixMultiplication-part2");
		job1.setJarByClass(All.class);
		job1.setMapperClass(MatrixMapper2_2.class);
		job1.setCombinerClass(MatrixReducer2_2.class);
		job1.setReducerClass(MatrixReducer2_2.class);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job1, new Path("outputIntermediate"));
		fs = FileSystem.get(conf);
		if (fs.exists(new Path(outputFolder)))
            fs.delete(new Path(outputFolder), true);
		FileOutputFormat.setOutputPath(job1, new Path(outputFolder));
		job1.waitForCompletion(true);
		
		

	}


	
	public static class CalculateNormMapper extends Mapper<Object, Text, Text, Text> 

	{

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException 

		{

			Configuration conf = context.getConfiguration(); 

			float val = Float.parseFloat(value.toString().split("\t")[1]);
			val= val*val;
			context.write(new Text("1"),new Text(Float.toString(val)));
			float currentNorm = conf.getFloat("Norm", (float) 0.0);
			currentNorm+=val;
			conf.setFloat("Norm", currentNorm);
			
		}
	}


	public static class CalculateNormReducer extends Reducer<Text, Text, Text, Text> 

	{

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException 

		{

			String value;
			float result = (float) 0.0;
			for (Text val : values) 

			{

				value = val.toString();
				result += Float.parseFloat(value); 

			}
			
			context.write(key, new Text(Float.toString(result)));

		}

	}
	

	public static class NormalizeMapper extends Mapper<Object, Text, Text, Text> 

	{

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException 

		{

			context.write(new Text(value.toString().split("\t")[0]), new Text(value.toString().split("\t")[1]));
			
			
		}
	}


	public static class NormalizeReducer extends Reducer<Text, Text, Text, Text> 

	{

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException 

		{
			Configuration conf = context.getConfiguration(); 
			
			float Norm = (float) Math.sqrt(conf.getFloat("Norm",1));
			
			float v = (float) 0.0;
			for (Text val : values) 

			{

				String value= val.toString();
				v += Float.parseFloat(value); 

			}
			v/=Norm;
			context.write(key, new Text(Float.toString(v)));

		}

	}
	

	
	
	
static void normalizeVectorUsingHadoop(String inputR) throws IOException, ClassNotFoundException, InterruptedException {
	
	Configuration conf = new Configuration();
	
	conf.setFloat("Norm", (float) 0.0);
	
	Job job1  = Job.getInstance(conf, "NormCalculation");
	job1.setJarByClass(All.class);
	job1.setMapperClass(CalculateNormMapper.class);
	job1.setCombinerClass(CalculateNormReducer.class);
	job1.setReducerClass(CalculateNormReducer.class);
	job1.setOutputKeyClass(Text.class);
	job1.setOutputValueClass(Text.class);
	FileInputFormat.addInputPath(job1, new Path(inputR));
	FileSystem fs = FileSystem.get(conf);
	if (fs.exists(new Path("Norm")))
        fs.delete(new Path("Norm"), true);
	FileOutputFormat.setOutputPath(job1, new Path("Norm"));
	job1.waitForCompletion(true);


	
	job1  = Job.getInstance(conf, "NormalizeVector");
	job1.setJarByClass(All.class);
	job1.setMapperClass(NormalizeMapper.class);
	job1.setReducerClass(NormalizeReducer.class);
	job1.setOutputKeyClass(Text.class);
	job1.setOutputValueClass(Text.class);
	FileInputFormat.addInputPath(job1, new Path(inputR));
	fs = FileSystem.get(conf);
	if (fs.exists(new Path("Norm")))
        fs.delete(new Path("Norm"), true);
	FileOutputFormat.setOutputPath(job1, new Path("Norm"));
	job1.waitForCompletion(true);
	
	if (fs.exists(new Path(inputR)))
        fs.delete(new Path(inputR), true);
	
	fs.rename(new Path("Norm"), new Path(inputR));
	

	
}



	public static void main( String[] args) throws ClassNotFoundException, IOException, InterruptedException

	{

//		
//		int mostImportantUsingHadoop = getMostImportantPageUsingHadoop("input","inputR", "out", 5);
//		int mostImpotantUsingArray = 0;//getMostImportantPageUsingSparkUsingArray("input.txt","out2.txt", 2);
//
//		System.out.println("is1 = " + mostImportantUsingHadoop + ", is2 = " + mostImpotantUsingArray);
		
		//matrixMultiplicationUsingOneMapReduce("input","inputR","out");
		
		//matrixMultiplicationUsingTwoMapReduce("input","inputR","out");

	}
}
