package org.clustering.utilities;

import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
//import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.util.Tool;


public class EdgeToCompressedAdjListConverter extends Configured implements Tool{

	public static class EdgeToAdjListMapper extends Mapper<LongWritable,Text,Text,Text> {
		private Text outputKey = new Text();
		private Text outputValue = new Text();

		protected void setup(Context context) throws IOException, InterruptedException {
			System.out.println("#################### EdgeToAdjListMapper ####################");
		}
		protected void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException{
			if(value.toString().startsWith("#"))
				return;
			String[] valueArr = value.toString().split("\t");		// handles weighted graphs as well.
			outputKey.set(valueArr[0]);
			if(valueArr.length == 2)
				outputValue.set(valueArr[1]);
			else
				outputValue.set(valueArr[1] + " " + valueArr[2]);
			context.write(outputKey, outputValue);
		}
	}
	
	public static class EdgeToAdjListCombiner extends Reducer<Text,Text,Text,Text>{
		private Text outputValue = new Text();
		protected void setup(Context context) throws IOException, InterruptedException{
			System.out.println("#################### EdgeToAdjListCombiner ####################");
		}
		protected void reduce(Text key,Iterable<Text> values,Context context) throws IOException, InterruptedException{
			StringBuffer sb = new StringBuffer();
			for(Text v:values)
				sb.append(" ").append(v.toString());
			
			if(sb.length() > 1){
				sb.deleteCharAt(0);
				outputValue.set(sb.toString());
				context.write(key, outputValue);
			}
		}
	}
	//modify the reducer to compress the adjList.
	public static class EdgeToAdjListReducer extends Reducer<Text,Text,Text,Text>{
		private Text outputValue = new Text();
		protected void setup(Context context) throws IOException, InterruptedException{
			System.out.println("#################### EdgeToAdjListReducer ####################");
		}
		protected void reduce(Text key,Iterable<Text> values,Context context) throws IOException, InterruptedException{
			StringBuffer sb = new StringBuffer();
			for(Text v:values)
				sb.append(" ").append(v.toString());
			
			if(sb.length() > 1){
				sb.deleteCharAt(0);
				outputValue.set(sb.toString());
				context.write(key, outputValue);
			}
		}
	}

	protected Path edgeListPath;
	protected Path adjListPath;
	protected int NUM_REDUCERS;
	protected String isWeighted;
	@Override
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		//args includes edge file path(0),adjList file path(1), number of reducers.
		if(args.length != 3)
			return -1;
		edgeListPath = new Path(args[0]);
		adjListPath = new Path(args[1]);
		NUM_REDUCERS = Integer.parseInt(args[2]);
		isWeighted = args[3]; 
		Job job = Configure();
		job.waitForCompletion(true);
		return 0;
	}
	
	public Job Configure() throws Exception{
		Configuration conf = getConf();
		conf.set("isWeighted", isWeighted);
		Job job = new Job(conf,"EDGE_TO_COMP_ADJLIST_STAGE");
		job.setJarByClass(EdgeToAdjListConverter.class);
		job.setMapperClass(EdgeToAdjListMapper.class);
//		job.setCombinerClass(EdgeToAdjListCombiner.class);
		job.setReducerClass(EdgeToAdjListReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setNumReduceTasks(NUM_REDUCERS);
		FileInputFormat.addInputPath(job, edgeListPath);
		FileOutputFormat.setOutputPath(job,adjListPath);
		return job;
	}


}
