package inf583.wordcount_spark;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.lib.IdentityMapper;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.join.TupleWritable;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Part_A_Using_Hadoop {

	public static class MapperExercise1 extends Mapper<Object, Text, IntWritable, IntWritable> {

		private final static IntWritable one = new IntWritable(1);
		private IntWritable number = new IntWritable(1);

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

			number.set(Integer.parseInt(value.toString()));

			context.write(one, number);
		}
	}

	public static class ReducerExercise1 extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
		private IntWritable result = new IntWritable();

		public void reduce(IntWritable key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {

			int largest = 0;
			boolean isChanged = false;
			for (IntWritable val : values) {
				if (isChanged == false) {
					largest = val.get();
					isChanged = true;
				} else {
					largest = Math.max(largest, val.get());
				}

			}

			result.set(largest);
			context.write(key, result);
		}
	}

	public static class MapperExercise2 extends Mapper<Object, Text, IntWritable, DoubleWritable> {

		private final static IntWritable one = new IntWritable(1);
		private DoubleWritable number = new DoubleWritable(1);

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

			number.set(Integer.parseInt(value.toString()));

			context.write(one, number);
		}
	}

	public static class ReducerExercise2 extends Reducer<IntWritable, DoubleWritable, IntWritable, DoubleWritable> {
		private DoubleWritable result = new DoubleWritable();

		public void reduce(IntWritable key, Iterable<DoubleWritable> values, Context context)
				throws IOException, InterruptedException {

			double sum = 0;
			int count = 0;
			for (DoubleWritable val : values) {

				sum += val.get();
				count += 1;
			}
			
			result.set(sum / count);
			context.write(key, result);
		}
	}

	public static class MapperExercise3 extends Mapper<Object, Text, IntWritable, IntWritable> {

		private final static IntWritable one = new IntWritable(1);
		private IntWritable number = new IntWritable(1);

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

			number.set(Integer.parseInt(value.toString()));

			context.write(number, one);
		}
	}

	public static class ReducerExercise3 extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
		private IntWritable result = new IntWritable();

		public void reduce(IntWritable key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {

			result.set(1);
			context.write(key, result);
		}
	}

	public static class MapperExercise4 extends Mapper<Object, Text, IntWritable, IntWritable> {

		private final static IntWritable one = new IntWritable(1);
		private IntWritable number = new IntWritable(1);

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

			context.write(one, one);
		}
	}

	public static class ReducerExercise4 extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
		private IntWritable result = new IntWritable();

		public void reduce(IntWritable key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {

			int sum = 0;

			for (IntWritable val : values) {
				sum += val.get();
			}

			result.set(sum);
			context.write(key, result);
		}
	}

	static void getLargestIntegerUsingHadoop(String inputFile, String outputFolder)
			throws IOException, ClassNotFoundException, InterruptedException {

		Configuration conf = new Configuration();

		Job job1 = Job.getInstance(conf, "part-1");
		job1.setJarByClass(Part_A_Using_Hadoop.class);
		job1.setMapperClass(MapperExercise1.class);
		job1.setCombinerClass(ReducerExercise1.class);
		job1.setReducerClass(ReducerExercise1.class);
		job1.setOutputKeyClass(IntWritable.class);
		job1.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job1, new Path(inputFile));
		FileOutputFormat.setOutputPath(job1, new Path(outputFolder));
		job1.waitForCompletion(true);

	}

	static void getAverageOfAllIntegersUsingHadoop(String inputFile, String outputFolder)
			throws IOException, ClassNotFoundException, InterruptedException {

		Configuration conf = new Configuration();

		Job job2 = Job.getInstance(conf, "part-2");
		job2.setJarByClass(Part_A_Using_Hadoop.class);
		job2.setMapperClass(MapperExercise2.class);
		// no combiner
		job2.setReducerClass(ReducerExercise2.class);
		job2.setOutputKeyClass(IntWritable.class);
		job2.setOutputValueClass(DoubleWritable.class);
		FileInputFormat.addInputPath(job2, new Path(inputFile));
		FileOutputFormat.setOutputPath(job2, new Path(outputFolder));
		job2.waitForCompletion(true);
	}

	static void distinctFilterUsingHadoop(String inputFile, String outputFolder)
			throws IOException, ClassNotFoundException, InterruptedException {

		Configuration conf = new Configuration();

		Job job3 = Job.getInstance(conf, "part-3");
		job3.setJarByClass(Part_A_Using_Hadoop.class);
		job3.setMapperClass(MapperExercise3.class);
		job3.setCombinerClass(ReducerExercise3.class);
		job3.setReducerClass(ReducerExercise3.class);
		job3.setOutputKeyClass(IntWritable.class);
		job3.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job3, new Path(inputFile));
		FileOutputFormat.setOutputPath(job3, new Path(outputFolder));
		job3.waitForCompletion(true);
	}

	static void getNumberOfDistinctIntegerUsingHadoop(String inputFile, String outputFolder)
			throws IOException, ClassNotFoundException, InterruptedException {

		Configuration conf = new Configuration();

		Job job4 = Job.getInstance(conf, "part-4");
		job4.setJarByClass(Part_A_Using_Hadoop.class);
		job4.setMapperClass(MapperExercise4.class);
		job4.setCombinerClass(ReducerExercise4.class);
		job4.setReducerClass(ReducerExercise4.class);
		job4.setOutputKeyClass(IntWritable.class);
		job4.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job4, new Path(inputFile));
		FileOutputFormat.setOutputPath(job4, new Path(outputFolder));
		job4.waitForCompletion(true);
	}

	public static void main(String[] args) throws ClassNotFoundException, IOException, InterruptedException {
		getLargestIntegerUsingHadoop("integers.txt", "output1");
		getAverageOfAllIntegersUsingHadoop("integers.txt", "output2");
		distinctFilterUsingHadoop("integers.txt", "output3");
		getNumberOfDistinctIntegerUsingHadoop("output3", "output4");
	}
}
