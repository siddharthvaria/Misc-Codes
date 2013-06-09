import java.io.IOException;

import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
public class test extends Configured implements Tool{

	public static class map1 extends Mapper<LongWritable,Text,IntWritable,IntWritable> {
		private IntWritable outputKey = new IntWritable();
		private IntWritable outputValue = new IntWritable(1);

		protected void setup(Context context) throws IOException, InterruptedException {
			System.out.println("### ### ### map1 ### ### ###");
		}
		protected void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException{
			if(value.toString().startsWith("#"))
				return;
			String[] valueArr = value.toString().split("\t");
			outputKey.set(Integer.parseInt(valueArr[0]));
			context.write(outputKey, outputValue);
		}
	}
	
	public static class combine1 extends Reducer<IntWritable,IntWritable,IntWritable,IntWritable>{
		private IntWritable outputValue = new IntWritable();
		protected void setup(Context context) throws IOException, InterruptedException{
			System.out.println("### ### ### combine1 ### ### ###");
		}
		protected void reduce(IntWritable key,Iterable<IntWritable> values,Context context) throws IOException, InterruptedException{
			int pd = 0;
			for(IntWritable i:values)
				pd += i.get();
			outputValue.set(pd);
			context.write(key, outputValue);
		}
	}

	public static class reduce1 extends Reducer<IntWritable,IntWritable,IntWritable,IntWritable>{
		private IntWritable outputValue = new IntWritable();
		protected void setup(Context context) throws IOException, InterruptedException{
			System.out.println("### ### ### reduce1 ### ### ###");
		}
		protected void reduce(IntWritable key,Iterable<IntWritable> values,Context context) throws IOException, InterruptedException{
			int d = 0;
			for(IntWritable i:values)
				d += i.get();
			outputValue.set(d);
			context.write(key, outputValue);
		}
	}
	
	public static class map2 extends Mapper<Text,Text,ByteWritable,IntWritable>{
		private byte NUM_REDUCERS;
		private ByteWritable outputKey = new ByteWritable();
		private IntWritable outputValue = new IntWritable();
		protected void setup(Context context) throws IOException, InterruptedException{
			System.out.println("### ### ### map2 ### ### ###");
			Configuration conf = context.getConfiguration();
			NUM_REDUCERS = Byte.parseByte(conf.get("NUM_REDUCERS"));
		}
		protected void map(Text key,Text value,Context context) throws IOException, InterruptedException{
			outputKey.set((byte)(Integer.parseInt(key.toString()) % NUM_REDUCERS));
			outputValue.set(Integer.parseInt(value.toString()));
			context.write(outputKey,outputValue);
		}
	}
	public static class combine2 extends Reducer<ByteWritable,IntWritable,ByteWritable,IntWritable>{
		private IntWritable outputValue = new IntWritable();
		protected void setup(Context context) throws IOException, InterruptedException{
			System.out.println("### ### ### combine2 ### ### ###");
		}
		protected void reduce(ByteWritable key,Iterable<IntWritable> values,Context context) throws IOException, InterruptedException{
			int ps = 0;
			for(IntWritable i:values){
				ps += i.get();
			}
			outputValue.set(ps);
			context.write(key, outputValue);
		}
	}
	
	public static class reduce2 extends Reducer<ByteWritable,IntWritable,ByteWritable,IntWritable>{
		private IntWritable outputValue = new IntWritable();
		protected void setup(Context context) throws IOException, InterruptedException{
			System.out.println("### ### ### reduce2 ### ### ###");
		}
		protected void reduce(ByteWritable key,Iterable<IntWritable> values,Context context) throws IOException, InterruptedException{
			int ps = 0;
			for(IntWritable i:values){
				ps += i.get();
			}
			outputValue.set(ps);
			context.write(key, outputValue);
		}
	}
	
	byte NUM_REDUCERS;
	Path inputPath;
	Path tmpPath;
	Path outputPath;
	
	public static void main(String[] args) throws Exception{
		int ret = ToolRunner.run(new Configuration(),new test(),args);
		System.exit(ret);
	}

	@Override
	public int run(String[] args) throws Exception {
		inputPath = new Path(args[0]);
		tmpPath = new Path("tmp");
		outputPath = new Path(args[1]);
		NUM_REDUCERS = Byte.parseByte(args[2]);
		Job job1 = configStage1();
		job1.waitForCompletion(true);
		Job job2 = configStage2();
		job2.waitForCompletion(true);
		return 0;
	}
	
	public Job configStage1() throws Exception{
		Configuration conf = getConf();
		Job job = new Job(conf,"job1");
		job.setJarByClass(test.class);
		job.setMapperClass(map1.class);
		job.setCombinerClass(combine1.class);
		job.setReducerClass(reduce1.class);
		job.setNumReduceTasks(NUM_REDUCERS);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, inputPath);
		FileOutputFormat.setOutputPath(job, tmpPath);
		return job;
	}
	public Job configStage2() throws Exception{
		Configuration conf = getConf();
		conf.set("NUM_REDUCERS",Byte.toString(NUM_REDUCERS));
		Job job = new Job(conf,"job2");
		job.setJarByClass(test.class);
		job.setMapperClass(map2.class);
		job.setCombinerClass(combine2.class);
		job.setReducerClass(reduce2.class);
		job.setNumReduceTasks(NUM_REDUCERS);
		job.setMapOutputValueClass(ByteWritable.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(ByteWritable.class);
		job.setOutputValueClass(IntWritable.class);
		job.setInputFormatClass(KeyValueTextInputFormat.class);
		FileInputFormat.addInputPath(job, tmpPath);
		FileOutputFormat.setOutputPath(job, outputPath);
		return job;
	}
}
