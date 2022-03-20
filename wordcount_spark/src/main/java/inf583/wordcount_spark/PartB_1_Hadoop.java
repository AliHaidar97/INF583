package inf583.wordcount_spark;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class PartB_1_Hadoop {

	static int getMostImportantPageUsingHadoop(String inputFile, String inputRFile, String output,
			int numberOfIteration) throws IOException, ClassNotFoundException, InterruptedException {

		for (int i = 0; i < numberOfIteration; i++) {
			Configuration conf = new Configuration();
			Job job1 = Job.getInstance(conf, "part-B2");
			job1.setJarByClass(WordCount.class);
			job1.setReducerClass(ReducerExerciseWithoutArrayB2_1.class);
			job1.setOutputKeyClass(Text.class);
			job1.setOutputValueClass(Text.class);
			job1.setInputFormatClass(TextInputFormat.class);
			job1.setOutputFormatClass(TextOutputFormat.class);

			if (i == 0)
				MultipleInputs.addInputPath(job1, new Path(inputRFile), TextInputFormat.class,
						MapperExerciceVectorWithoutArrayB2_1.class);
			else
				MultipleInputs.addInputPath(job1, new Path(output), TextInputFormat.class,
						MapperExerciceVectorWithoutArrayB2_1.class);

			MultipleInputs.addInputPath(job1, new Path(inputFile), TextInputFormat.class,
					MapperExerciceMatrixWithoutArrayB2_1.class);

			FileSystem fs = FileSystem.get(conf);
			if (fs.exists(new Path("outputIntermediate")))
				fs.delete(new Path("outputIntermediate"), true);

			TextOutputFormat.setOutputPath(job1, new Path("outputIntermediate"));
			job1.waitForCompletion(true);
			job1 = Job.getInstance(conf, "part-B2");
			job1.setJarByClass(WordCount.class);
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

	public static class MapperExerciceMatrixWithoutArrayB2_1 extends Mapper<Object, Text, Text, Text> {

		private static Text firstNode = new Text();
		private static Text secondNode = new Text();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

			String[] list = value.toString().split(" ");

			for (int i = 1; i < list.length; i++) {
				firstNode.set(list[i]);
				secondNode.set("M#" + list[0] + "#" + "1");
				context.write(firstNode, secondNode);

			}
		}
	}

	public static class MapperExerciceVectorWithoutArrayB2_1 extends Mapper<Object, Text, Text, Text> {

		private static Text firstNode = new Text();
		private static Text secondNode = new Text();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

			String[] list = value.toString().split("	");
			firstNode.set(list[0]);
			secondNode.set("A#" + list[0] + "#" + list[1]);
			context.write(firstNode, secondNode);

		}
	}

	public static class ReducerExerciseWithoutArrayB2_1 extends Reducer<Text, Text, Text, Text> {

		private static Text firstNode = new Text();
		private static Text secondNode = new Text();

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

			ArrayList<String> list = new ArrayList<>();
			for (Text val : values) {
				list.add(val.toString());
			}
			Collections.sort(list);

			String val = list.get(0).split("#")[2];

			firstNode.set(key);
			secondNode.set("0");
			context.write(firstNode, secondNode);

			for (int i = 1; i < list.size(); i++) {
				firstNode.set(list.get(i).split("#")[1]);
				secondNode.set(val);
				context.write(firstNode, secondNode);
			}
		}
	}

	public static class MapperExerciceWithoutArrayB2_2 extends Mapper<Object, Text, Text, Text> {

		private static Text firstNode = new Text();
		private static Text secondNode = new Text();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

			String[] list = value.toString().split("\t");

			firstNode.set(list[0]);
			secondNode.set(list[1]);
			context.write(firstNode, secondNode);

		}
	}

	public static class ReducerExerciseWithoutArrayB2_2 extends Reducer<Text, Text, Text, Text> {

		private static Text firstNode = new Text();
		private static Text secondNode = new Text();

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

			double result = 0.0;

			for (Text val : values) {
				result += Double.parseDouble(val.toString());
			}

			secondNode.set(Double.toString(result));
			firstNode.set(key);

			context.write(firstNode, secondNode);

		}

	}

	public static class CalculateNormMapper extends Mapper<Object, Text, Text, Text>

	{
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException

		{

			Configuration conf = context.getConfiguration();

			float val = Float.parseFloat(value.toString().split("\t")[1]);
			val = val * val;
			context.write(new Text("1"), new Text(Float.toString(val)));
			float currentNorm = conf.getFloat("Norm", (float) 0.0);
			currentNorm += val;
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

			float Norm = (float) Math.sqrt(conf.getFloat("Norm", 1));

			float v = (float) 0.0;
			for (Text val : values)

			{

				String value = val.toString();
				v += Float.parseFloat(value);

			}
			v /= Norm;
			context.write(key, new Text(Float.toString(v)));

		}

	}

	static void normalizeVectorUsingHadoop(String inputR)
			throws IOException, ClassNotFoundException, InterruptedException {

		Configuration conf = new Configuration();

		conf.setFloat("Norm", (float) 0.0);

		Job job1 = Job.getInstance(conf, "NormCalculation");
		job1.setJarByClass(WordCount.class);
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

		job1 = Job.getInstance(conf, "NormalizeVector");
		job1.setJarByClass(WordCount.class);
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

}
