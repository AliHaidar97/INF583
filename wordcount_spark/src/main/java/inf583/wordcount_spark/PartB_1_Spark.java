package inf583.wordcount_spark;

import org.apache.spark.*;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;
import java.util.*;

import org.apache.spark.util.*;

public class PartB_1_Spark {

	static int getMostImportantPageUsingSparkUsingArray(String inputFile, String outputFolder, int numberOfIteration) {

		// Create a Java Spark Context
		SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("wordCount");
		JavaSparkContext sc = new JavaSparkContext(conf);

		// Load our input data.
		JavaRDD<String> input = sc.textFile(inputFile);

		int numberOfNodes = (input.mapToPair(a -> {
			return new Tuple2<>(1, Integer.parseInt(a.split(" ")[0]));
		})).reduceByKey((a, b) -> Math.max(a, b)).collect().get(0)._2 + 1;

		System.out.println("numberOfNodes = " + numberOfNodes);

		JavaRDD<String> edges = input.flatMap(s -> {
			String[] nodes = s.split(" ");
			for (int i = 1; i < nodes.length; i++)
				nodes[i] = nodes[0] + " " + nodes[i];
			return Arrays.asList(Arrays.copyOfRange(nodes, 1, nodes.length)).iterator();
		});

		JavaPairRDD<Integer, Tuple2<Integer, Double>> graph = edges.mapToPair(s -> {
			String[] edge = s.split(" ");
			return new Tuple2<>(Integer.parseInt(edge[0]), new Tuple2<>(Integer.parseInt(edge[1]), 1.0));
		});

		double[] arr = new double[numberOfNodes];

		for (int i = 0; i < numberOfNodes; i++) {
			arr[i] = 1.0 / numberOfNodes;

		}

		JavaPairRDD<Integer, Double> r;

		for (int i = 0; i < numberOfIteration; i++) {
			
			r = (graph.mapValues(a -> (new Tuple2<>(a._1, arr[a._1])))
					.reduceByKey((a, b) -> new Tuple2<>(a._1, (a._2 + b._2)))).mapValues(a -> new Double(a._2));

			double norm = 0;
			norm = r.mapToPair(a -> {
				return new Tuple2<>(1, a._2 * a._2);
			}).reduceByKey((a, b) -> a + b).lookup(1).get(0);

			for (int j = 0; j < numberOfNodes; j++) {
				arr[j] = 0;
			}

			for (Tuple2<Integer, Double> val : r.collect()) {

				arr[val._1] = val._2 / norm;

			}

		}
		int bestNode = 0;
		for (int i = 0; i < numberOfNodes; i++) {
			if (arr[bestNode] < arr[i]) {
				bestNode = i;
			}
		}
		return bestNode;

	}

	static int getMostImportantPageUsingSparkUsingOnlyRDD(String inputMatrixFile, String inputRFile,
			String outputFolder, int numberOfIteration) {

		// Create a Java Spark Context
		SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("wordCount");
		JavaSparkContext sc = new JavaSparkContext(conf);

		// Load our input data.
		JavaRDD<String> inputMatrix = sc.textFile(inputMatrixFile);
		JavaRDD<String> inputR = sc.textFile(inputRFile);
		long numberOfNodes = (inputR.count());

		System.out.println("numberOfNodes = " + numberOfNodes);

		JavaRDD<String> edges = inputMatrix.flatMap(s -> {
			String[] nodes = s.split(" ");
			for (int i = 1; i < nodes.length; i++)
				nodes[i] = nodes[0] + " " + nodes[i];
			return Arrays.asList(Arrays.copyOfRange(nodes, 1, nodes.length)).iterator();
		});

		JavaPairRDD<String, String> graph = edges.mapToPair(s -> {
			String[] edge = s.split(" ");
			return new Tuple2<>(edge[0] + "#" + edge[1], "M#1.0");
		});

		JavaRDD<String> r = inputR;

		for (int i = 0; i < numberOfIteration; i++) {

			JavaPairRDD<String, String> rTransform = r.flatMap(s -> {
				ArrayList<String> temp = new ArrayList<>();
				for (int j = 0; j < temp.size(); j++) {
					temp.add(j + "#" + s.split(" ")[0] + " " + "V#" + s.split(" ")[1]);
				}
				return temp.iterator();
			}).mapToPair(s -> {
				return new Tuple2<>(s.split(" ")[0], s.split(" ")[1]);
			});

			rTransform.saveAsTextFile("outputR");

			JavaPairRDD<String, String> tempR = graph.union(rTransform).reduceByKey((a, b) -> {
				double result = Double.parseDouble(a.split("#")[1]) * Double.parseDouble(b.split("#")[1]);
				return "M#result";
			}).mapToPair(s -> {
				if (s._2.split("#")[0] == "M")
					return new Tuple2<>(s._1.split("#")[0], s._2.split("#")[1]);
				return new Tuple2<>(s._1.split("#")[0], "0.0");
			}).reduceByKey((a, b) -> {
				return Double.toString(Double.parseDouble(a) + Double.parseDouble(b));
			});
			double norm = tempR.mapToPair(a -> {
				return new Tuple2<>(1, Math.pow(Double.parseDouble(a._2), 2));
			}).reduceByKey((a, b) -> {
				return (a + b);
			}).collect().get(0)._2;
			tempR = tempR.mapValues(a -> Double.toString(Double.parseDouble(a) / norm));
			tempR.map(s -> (s._1 + " " + s._2));
		}

		String bestNode = r.mapToPair(s -> {
			return new Tuple2<>(1, s);
		}).reduceByKey((a, b) -> {
			if (Double.parseDouble(a.split(" ")[1]) > Double.parseDouble(b.split(" ")[1])) {
				return a;
			}
			return b;
		}).collect().get(0)._2;
		sc.stop();
		return Integer.parseInt(bestNode.split(" ")[0]);

	}
	
	public static void main(String args[]) {
		
		int it=6;
		int is = getMostImportantPageUsingSparkUsingArray("input","output",it);
		System.out.println("The most important page_"+it+" : " + is);
		
	}

}
