package org.clustering.unweighted.gc;

import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
//import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
//import org.apache.hadoop.mapreduce.Mapper;
//import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
//import org.apache.hadoop.util.*;

import org.clustering.utilities.*;

public class GRAPHCOLORING extends Configured implements Tool{

	protected static enum coloredNodesCounter{
		coloredNodes
	}

	public static class gcMapper1 extends Mapper<Text,Text,Text,IntArrayWritable>{
		
		private IntArrayWritable outputValue = new IntArrayWritable();
		private Text outputKey = new Text();
		protected void setup(Context context) throws IOException, InterruptedException{
			System.out.println("### ### ### Graph Coloring Mapper 1 ### ### ###");
		}
		protected void map(Text key,Text value,Context context) throws IOException, InterruptedException{
			
			/*
			Input is of the form:
			<k,v> where k = nodeId and v = adjList of the form v1 w1 v2 w2...
			Delimiter between k and v is \t and delimiter between vi wi is single white space.
			*/
			
			int i;
			String[] valueStrings = value.toString().split(" ");
			int[] tmp = new int[2];
			tmp[0] = Integer.parseInt(key.toString());
			tmp[1] = (int) (Math.random() * Integer.MAX_VALUE);
			outputValue.set(tmp);
			for(i = 0;i < valueStrings.length;i = i + 2){		//incrementing by 2 to ignore the edge weights
				outputKey.set(valueStrings[i]);
				context.write(outputKey,outputValue);
			}
			//emit to self
			outputKey.set(key);
			context.write(outputKey,outputValue);
		}
	}
	
	public static class gcReducer1 extends Reducer<Text,IntArrayWritable,Text,Text>{
		private int ccv;
		private Text outputValue = new Text();
		protected void setup(Context context) throws IOException, InterruptedException{
			System.out.println("### ### ### Graph Coloring Reducer 1 ### ### ###");
			// obtain the current color from the configuration file.
			Configuration conf = context.getConfiguration();
			ccv = conf.getInt("CURRENT_COLOR_VALUE",ccv);
		}
		
		protected void reduce(Text key,Iterable<IntArrayWritable> values,Context context) throws IOException, InterruptedException{
			int k = Integer.parseInt(key.toString());
			int[] tmp;
			int minRandomValue = Integer.MAX_VALUE;
			int randomValue = 0;
			StringBuffer sb = new StringBuffer();
			for(IntArrayWritable iaw : values){
				tmp = iaw.get();
				if(tmp[0] == k){
					randomValue = tmp[1];
					if(randomValue < minRandomValue){
						minRandomValue = randomValue;
					}					
				}
				else{
					sb.append(" ").append(tmp[0]);
					if(tmp[1] < minRandomValue){
						minRandomValue = tmp[1];
					}		
				}
			}
			//sb.deleteCharAt(0);
			if(randomValue == minRandomValue){
				context.getCounter(coloredNodesCounter.coloredNodes).increment(1);
				sb.insert(0,Integer.toString(ccv));
			}
			else{
				sb.insert(0,Integer.toString(-1));
			}
			outputValue.set(sb.toString());
			context.write(key, outputValue);			
		}
	}
	public static class gcMapper2 extends Mapper<Text,Text,Text,IntArrayWritable>{
//		private int inc;
		private Text outputKey = new Text();
		private IntArrayWritable outputValue = new IntArrayWritable();
		protected void setup(Context context) throws IOException, InterruptedException{
			
			System.out.println("### ### ### Graph Coloring Mapper 2 ### ### ###");			
		}
		protected void map(Text key,Text value,Context context) throws IOException, InterruptedException{
			int i;
			int[] tmp = new int[2];
			String[] valueStrings = value.toString().split(" ");
			int[] keyColor = {Integer.parseInt(key.toString()),Integer.parseInt(valueStrings[0])};
//			if(keyColor[1] < 0 && keyColor[1] > Integer.MIN_VALUE){		// node is already colored
			/*
			  map phase emits either <key,random value> or <key,-1> based on whether the node is colored or not.
			  If the node is colored then it emits its color to itself and -1 to its neighbors
			  If the node is uncolored then it emits its random value to its neighbors
			*/
			if(keyColor[1] < -1){
				tmp[0] = keyColor[0];
				tmp[1] = -1;
				outputValue.set(tmp);
				for(i = 1;i < valueStrings.length;i++){
					outputKey.set(valueStrings[i]);
					context.write(outputKey,outputValue);
				}
				//emit color to self
				outputValue.set(keyColor);
				context.write(key, outputValue);
			}
			else{
				tmp[0] = keyColor[0];
				tmp[1] = (int) ((Math.random() * Integer.MAX_VALUE));
				outputValue.set(tmp);
				for(i = 1;i < valueStrings.length;i++){
					outputKey.set(valueStrings[i]);
					context.write(outputKey, outputValue);
				}
				//emit random value to self
				context.write(key, outputValue);
			}
		}
	}
	public static class gcReducer2 extends Reducer<Text,IntArrayWritable,Text,Text>{
//		private int inc;
		private Text outputValue = new Text();
		private int ccv;
		protected void setup(Context context) throws IOException, InterruptedException{
			
			System.out.println("### ### ### Graph Coloring Reducer 2 ### ### ###");
			// obtain the current color from the configuration file.
			Configuration conf = context.getConfiguration();
			ccv = conf.getInt("CURRENT_COLOR_VALUE",ccv);			
		}
		protected void reduce(Text key,Iterable<IntArrayWritable> values,Context context) throws IOException, InterruptedException{
			int[] tmp;
			int k = Integer.parseInt(key.toString());
			int minRandomValue = Integer.MAX_VALUE;
			int randomValue = 0;
			StringBuffer sb = new StringBuffer();
			//ArrayList<Integer> newAdjList = new ArrayList<Integer>();
			for(IntArrayWritable iaw : values){
				tmp = iaw.get();
				if(tmp[0] == k){
					/*
					if tmp[1] is positive then its a random value
					if tmp[1] is negative then its a color value
					*/
					if(tmp[1] >= 0){
						randomValue = tmp[1];
						if(randomValue < minRandomValue){
							minRandomValue = randomValue;
						}
					}
					else{
						outputValue.set(Integer.toString(tmp[1]));
						context.write(key,outputValue);
						return;
					}
				}
				else{
					//tmp[1] is either -1 or some positive integer.
					if(tmp[1] >= 0){
						sb.append(" ").append(tmp[0]);
						if(tmp[1] < minRandomValue){
							minRandomValue = tmp[1];
						}
					}
				}
			}
			// check if the node will be colored,if it gets colored then increment the counter value.
			//sb.deleteCharAt(0);
			if(minRandomValue == randomValue){
				//increment the counter value
				context.getCounter(coloredNodesCounter.coloredNodes).increment(1);
				sb.insert(0,Integer.toString(ccv));
			}
			else{
				sb.insert(0,Integer.toString(-1));
			}
			outputValue.set(sb.toString());
			context.write(key, outputValue);
		} 
	}
	
	//other parameters
	protected Path adjListPath;
	protected Path gcTempPath = new Path("gctmp");
	protected Path outputPath;
	protected int CURRENT_COLOR_VALUE;
	protected int NUM_REDUCERS;
	protected String isWeighted;
	
	@Override
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		//args includes adjList file path(0), output file path(1),total number of nodes(2) & number of reducers(4).
		
		if(args.length != 5)
			return -1;
		adjListPath = new Path(args[0]);
		outputPath = new Path(args[1]);
		int NUM_NODES = Integer.parseInt(args[2]);
		NUM_REDUCERS = Integer.parseInt(args[3]);
		isWeighted = args[4];
		double FRACTION_OF_NODES_COLORED = 0.7;// holds fraction of total nodes that are to be colored.
		
		
		CURRENT_COLOR_VALUE = -(int)'a'; 	//nodes are colored -'a',-'b',-'c' and so on
		int iter = 0;
		int coloredNodes = 0;
		
		FileSystem fs = FileSystem.get(getConf());
		
		Job job = Configure1();
		job.waitForCompletion(true);
		Counters counters = job.getCounters();
		Counter counter = counters.findCounter(coloredNodesCounter.coloredNodes);
		coloredNodes += (int) counter.getValue();
		iter++;
		System.out.println("Number of nodes colored after " + iter + " iterations:" + coloredNodes);
		CURRENT_COLOR_VALUE--;

		while(coloredNodes < (FRACTION_OF_NODES_COLORED * NUM_NODES)){
			job = Configure2();
			job.waitForCompletion(true);
			counters = job.getCounters();
			counter = counters.findCounter(coloredNodesCounter.coloredNodes);
			coloredNodes += (int) counter.getValue();
			iter++;
			System.out.println("Number of nodes colored after " + iter + " iterations:" + coloredNodes);
			CURRENT_COLOR_VALUE--;
			fs.delete(gcTempPath, true);
			fs.rename(outputPath, gcTempPath);
		}
		fs.rename(gcTempPath, outputPath);
		return iter;
	}

	protected Job Configure1() throws Exception{
		Configuration conf = getConf();
		conf.setInt("CURRENT_COLOR_VALUE",CURRENT_COLOR_VALUE);
//		conf.set("isWeighted", isWeighted);
		Job job = new Job(conf,"GRAPH_COLORING1");
		job.setJarByClass(GRAPHCOLORING.class);
		job.setMapperClass(gcMapper1.class);
		job.setReducerClass(gcReducer1.class);
		job.setNumReduceTasks(NUM_REDUCERS);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntArrayWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setInputFormatClass(KeyValueTextInputFormat.class);
		FileInputFormat.addInputPath(job, adjListPath);
		FileOutputFormat.setOutputPath(job, gcTempPath);
		return job;
	}
	
	protected Job Configure2() throws Exception{
		Configuration conf = getConf();
		conf.setInt("CURRENT_COLOR_VALUE",CURRENT_COLOR_VALUE);
//		conf.set("isWeighted", isWeighted);
		Job job = new Job(conf,"GRAPH_COLORING2");
		job.setJarByClass(GRAPHCOLORING.class);
		job.setMapperClass(gcMapper2.class);
		job.setReducerClass(gcReducer2.class);
		job.setNumReduceTasks(NUM_REDUCERS);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntArrayWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setInputFormatClass(KeyValueTextInputFormat.class);
		FileInputFormat.addInputPath(job, gcTempPath);
		FileOutputFormat.setOutputPath(job, outputPath);
		return job;
	}
}