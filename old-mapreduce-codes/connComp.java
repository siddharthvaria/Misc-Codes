//hadoop
import org.apache.hadoop.conf.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
//java
import java.io.IOException;
//import java.util.HashMap;
public class connComp extends Configured implements Tool{
	
	protected static enum connCompCounter{
		CONVERGE_CHECK
	}
	public static class Mapper1 extends Mapper<LongWritable,Text,Text,Text>{
		private Text outputValue = new Text();
		private Text outputKey = new Text();
		protected void setup(Context context) throws IOException, InterruptedException{
			System.out.println("#################### Mapper1 ####################");
		}
		protected void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException{
			String[] tmp = value.toString().split("\t");
			outputKey.set(tmp[0]);
			outputValue.set(tmp[1] + ":" + tmp[1]);
			context.write(outputKey, outputValue);
		}
	}
/*	
 	public static class Reducer1 extends Reducer<Text,Text,Text,Text>{
		protected void setup(Context context) throws IOException, InterruptedException{
			System.out.println("#################### Reducer1 ####################");			
		}
		protected void reduce(Text key,Text value,Context context){
			
		}
	}
*/	
	public static class Mapper2 extends Mapper<LongWritable,Text,Text,Text>{
		private Text outputKey = new Text();
		private Text outputValue = new Text();
		protected void setup(Context context) throws IOException, InterruptedException{
			System.out.println("#################### Mapper2 ####################");			
		}
		protected void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException{
			String[] tmp = value.toString().split("\t");
			String msg = tmp[1].split(":")[0];
			String adjListStr = tmp[1].split(":")[1];
			String[] adjList = adjListStr.split(" ");
			outputValue.set(msg);
			for(String s:adjList){
				outputKey.set(s);
				context.write(outputKey, outputValue);
			}
			outputKey.set(tmp[0]);
			outputValue.set(tmp[0]);
			context.write(outputKey, outputValue);
			outputValue.set("o" + msg + ":" + adjListStr );
			context.write(outputKey, outputValue);
		}
	}
	public static class Reducer2 extends Reducer<Text,Text,Text,Text>{
		
		private int NUM_NODES;
		private Text outputValue = new Text();
		
		protected void setup(Context context) throws IOException, InterruptedException{
			System.out.println("#################### Reducer2 ####################");
			Configuration conf = context.getConfiguration();
			NUM_NODES = Integer.parseInt(conf.get("NUM_NODES"));
		}
		protected void reduce(Text key,Iterable<Text>values,Context context) throws IOException, InterruptedException{
			String oldMsg = "";
			String adjList = "";
			String[] tmp;
			StringBuffer sb = new StringBuffer();
			for(Text val:values){
				sb.append(",").append(val.toString());
			}
			
			sb.deleteCharAt(0);
			
			String[] valuesArr = sb.toString().split(",");
			
			sb.delete(0,sb.length());			// delete the contents of StringBuffer
			
//			HashMap<String,Integer> hm = new HashMap<String,Integer>(NUM_NODES);
			boolean[] change = new boolean[NUM_NODES + 1];
			
			int i;
			
			for(String s:valuesArr){
				if(s.startsWith("o")){
					tmp = s.substring(1).split(":");
					oldMsg = tmp[0];
					adjList = tmp[1];
				}
				else{
					tmp = s.split(" ");
					for(String t:tmp){
/*
						if(!hm.containsKey(t)){
							hm.put(t,1);
							sb.append(" ").append(t);
						}
*/
						i = Integer.parseInt(t);
							change[i] = true;
					}
				}
			}
//			sb.deleteCharAt(0);
			
			for(i = 1;i < change.length;i++){
				if(change[i])
					sb.append(" ").append(i);
			}
				
			sb.deleteCharAt(0);
				
			tmp = oldMsg.split(" ");
			for(String t:tmp){
/*				
				if(hm.containsKey(t))
					hm.put(t,0);
				else
					hm.put(t,1);
*/					
				i = Integer.parseInt(t);
				change[i] = false;
			}
/*			
			if(hm.containsValue(1))
				context.getCounter(connCompCounter.CONVERGE_CHECK).increment(1);
*/			
			for(i = 1;i < change.length;i++)
				if(change[i]){
					context.getCounter(connCompCounter.CONVERGE_CHECK).increment(1);
					break;
				}
			outputValue.set(sb.toString() + ":" + adjList);
			context.write(key, outputValue);
		}
	}
	
	public static class Mapper3 extends Mapper<LongWritable,Text,Text,Text>{
		
		private Text outputKey = new Text();
		private Text outputValue = new Text();
		
		protected void setup(Context context) throws IOException, InterruptedException{
			System.out.println("#################### Mapper3 ####################");
		}
		
		protected void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException{
			String[] tmp = value.toString().split("\t");
			outputKey.set(tmp[1].split(":")[0]);						// swap key value pairs
			outputValue.set(tmp[0]);
			context.write(outputKey, outputValue);
		}
	}
	
	public static class Combiner3 extends Reducer<Text,Text,Text,Text>{
		
		private Text outputValue = new Text();

		protected void setup(Context context) throws IOException, InterruptedException{
			System.out.println("#################### Combiner3 ####################");
		}

		protected void reduce(Text key,Iterable<Text> values,Context context) throws IOException, InterruptedException{
			StringBuffer sb = new StringBuffer();
			for(Text t:values){
				sb.append(" ").append(t.toString());
			}
			sb.deleteCharAt(0);
			outputValue.set(sb.toString());
			context.write(key,outputValue);
		}
	}
	
	public static class Reducer3 extends Reducer<Text,Text,Text,Text>{
		
		private Text outputKey = new Text("CC");
		private Text outputValue = new Text();
		
		protected void setup(Context context) throws IOException, InterruptedException{
			System.out.println("#################### Reducer3 ####################");
		}
		
		protected void reduce(Text key,Iterable<Text> values,Context context) throws IOException, InterruptedException{
			StringBuffer sb = new StringBuffer();
			for(Text t:values){
				sb.append(" ").append(t.toString());
			}
			sb.deleteCharAt(0);
			outputValue.set(sb.toString());
			context.write(outputKey, outputValue);
		}

	}

	//	command line arguments
	//	Usage: connComp inputPath outputPath NUM_NODES NUM_REDUCERS
	
	protected String NUM_NODES;
	protected int NUM_REDUCERS;
	protected Path inputPath;
	protected Path outputPath;
	protected Path tmpPath = new Path("tmp");
	
	public static void main(String[] args) throws Exception {
	int ret = ToolRunner.run(new Configuration(),new connComp(),args);	
	System.exit(ret);
	}
	@Override
	public int run(String[] args) throws Exception {
		if(args.length != 4){
			System.out.println("Usage:connComp inputPath outputPath NUM_NODES NUM_REDUCERS");
			return -1;
		}
		
		inputPath = new Path(args[0]);
		outputPath = new Path(args[1]);
		NUM_NODES = args[2];
		NUM_REDUCERS = Integer.parseInt(args[3]);
		
		FileSystem fs = FileSystem.get(getConf());
		
		Job job1 = configStage1();
		job1.waitForCompletion(true);
		
		fs.delete(inputPath,true);
		fs.rename(outputPath,inputPath);
		
		while(true){
			Job job2 = configStage2();
			job2.waitForCompletion(true);
			Counters counters = job2.getCounters();
			Counter counter = counters.findCounter(connCompCounter.CONVERGE_CHECK);
			long change = counter.getValue();
			if(change == 0){
				fs.delete(inputPath,true);
				fs.rename(outputPath,inputPath);
				break;
			}
			fs.delete(inputPath,true);
			fs.rename(outputPath, inputPath);
		}
		
		Job job3 = configStage3();
		job3.waitForCompletion(true);
		
		return 0;
	}
	private Job configStage1() throws Exception{
		Configuration conf = getConf();
		Job job = new Job(conf,"STAGE1");
		job.setJarByClass(connComp.class);
		job.setMapperClass(Mapper1.class);
		job.setNumReduceTasks(0);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, inputPath);
		FileOutputFormat.setOutputPath(job, outputPath);
		return job;
	}
	private Job configStage2() throws Exception{
		Configuration conf = getConf();
		conf.set("NUM_NODES", NUM_NODES);
		Job job = new Job(conf,"STAGE2");
		job.setJarByClass(connComp.class);
		job.setMapperClass(Mapper2.class);
		job.setReducerClass(Reducer2.class);
		job.setNumReduceTasks(NUM_REDUCERS);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, inputPath);
		FileOutputFormat.setOutputPath(job, outputPath);
		return job;
	}
	
	private Job configStage3() throws Exception{
		Configuration conf = getConf();
		Job job = new Job(conf,"STAGE2");
		job.setJarByClass(connComp.class);
		job.setMapperClass(Mapper3.class);
		job.setCombinerClass(Combiner3.class);
		job.setReducerClass(Reducer3.class);
		job.setNumReduceTasks(NUM_REDUCERS);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, inputPath);
		FileOutputFormat.setOutputPath(job, outputPath);
		return job;
	}
}