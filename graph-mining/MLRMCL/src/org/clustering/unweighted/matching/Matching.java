package org.clustering.unweighted.matching;

import java.io.IOException;
//import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
//import org.apache.hadoop.mapreduce.Job;
//import org.apache.hadoop.mapreduce.Mapper;
//import org.apache.hadoop.mapreduce.Reducer;
//import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
//import org.clustering.utilities.*;

public class Matching extends Configured implements Tool {
	
	protected static enum matchingCounter{
		matchedNodes
	}
	
	protected static enum failureCounter{
		failedMatches;
	}
	
	public static class MatchingMapper1 extends Mapper<Text,Text,IntWritable,Text>{
		private IntWritable outputKey = new IntWritable();
		private Text outputValue = new Text();
		protected void setup(Context context) throws IOException,	InterruptedException{
			System.out.println("### ### ### Matching Mapper1 ### ### ###");
		}
		protected void map(Text key,Text value,Context context) throws IOException, InterruptedException{
			outputKey.set(Integer.parseInt(key.toString()));
			String[] valArr = value.toString().split(" ",2);
			if(Integer.parseInt(valArr[0]) < 0){
				//its a key value pair from colors file
				outputValue.set(valArr[0]);
				context.write(outputKey, outputValue);
			}
			else{
				context.write(outputKey, value);
			}
		}
	}
	public static class MatchingReducer1 extends Reducer<IntWritable,Text,IntWritable,Text>{
		private Text outputValue = new Text();
		protected void setup(Context context) throws IOException,	InterruptedException{
			System.out.println("### ### ### Matching Reducer1 ### ### ###");
		}
		protected void reduce(IntWritable key,Iterable<Text> values,Context context) throws IOException, InterruptedException{
			int tmp;
			StringBuffer cAdj = new StringBuffer();
			for(Text t : values){
				String[] tArr = t.toString().split(" ",2);
				 tmp = Integer.parseInt(tArr[0]);
				if(tmp >= 0){
					cAdj.append(t.toString());
				}
				else{
					cAdj.insert(0,t.toString() + ":" + Integer.toString(-1) + ":");
				}	
			}
			outputValue.set(cAdj.toString());
			context.write(key, outputValue);
		}
	}
	public static class MatchingMapper2 extends Mapper<Text,Text,IntWritable,Text>{
		private int ccv;
		private IntWritable outputKey = new IntWritable();
		private Text outputValue = new Text();
		protected void setup(Context context) throws IOException,	InterruptedException{
			System.out.println("### ### ### Matching Mapper2 ### ### ###");
			Configuration conf = context.getConfiguration();
			ccv = conf.getInt("CURRENT_COLOR_VALUE",ccv);
		}
		protected void map(Text key,Text value,Context context) throws IOException, InterruptedException{
			int k = Integer.parseInt(key.toString());
//			String[] valArr1 = value.toString().split(" ",2);
			String[] valArr = value.toString().split(":");
			int color = Integer.parseInt(valArr[0]);
//			int tmp = Integer.parseInt(valArr1[0]);
//			if(tmp < 0){
			if(color < 0){
//				String[] valArr2 = valArr1[1].split(" ",2);
				int mv = Integer.parseInt(valArr[1]);
//				int mv = Integer.parseInt(valArr2[0]);
				// variable color holds some color value
				// perform matching for the current color value
				if(color == ccv){
//				if(tmp == ccv){
					if(mv == -1 && valArr.length > 2){
//					if(mv == -1 && valArr2.length > 1){
						// not yet matched 
						//String[] adjList = valArr2[1].split(" ");
						String[] adjList = valArr[2].split(" ");
						mv = Integer.parseInt(adjList[(int) (Math.random() * adjList.length)]);
						outputKey.set(mv);
						outputValue.set(key.toString() + ":" + Integer.toString(1));
						context.write(outputKey,outputValue);
					}
					outputKey.set(k);
					//outputValue.set(valArr1[0] + " " + Integer.toString(mv));
					outputValue.set(valArr[0] + ":" + Integer.toString(mv));
					context.write(outputKey, outputValue);
				}
				else{
					outputKey.set(k);
					context.write(outputKey,value);
				}
			}
			else{
				//variable color holds some node id.Split value again upto the end
				//perform synchronization for the previous color value
				int i;
				//valArr1 = value.toString().split(" ");
				//valArr = value.toString().split(":");
				//for(i = 0;i < valArr1.length;i++){
				for(i = 0;i < valArr.length;i++){	
					//outputKey.set(Integer.parseInt(valArr1[i]));
					outputKey.set(Integer.parseInt(valArr[i]));
					//outputValue.set(key.toString() + " " + Integer.toString(-1));
					outputValue.set(key.toString() + ":" + Integer.toString(-1));
					context.write(outputKey, outputValue);
				}
			}
		}
	}
	public static class MatchingReducer2 extends Reducer<IntWritable,Text,IntWritable,Text>{
		private Text outputValue = new Text();
		protected void setup(Context context) throws IOException,	InterruptedException{
			System.out.println("### ### ### Matching Reducer2 ### ### ###");
		}
		protected void reduce(IntWritable key,Iterable<Text> values,Context context) throws IOException, InterruptedException{
			int failedMatchNode = -1;	//initialization to -1 is important
			//ArrayList<Integer> nodesList = new ArrayList<Integer>();
			int minNodeId = Integer.MAX_VALUE;
			int mv = -1;
			int color = 0;
			int count = 0;
			String adjList = "";
			StringBuffer sb = new StringBuffer();
			for(Text t : values){
				//String[] tArr1 = t.toString().split(" ",2);
				String[] tArr = t.toString().split(":");
				int tmp = Integer.parseInt(tArr[0]);
				if(tmp < 0){
					//tmp is some color value
					color = tmp;
					//String[] tArr2 = tArr1[1].split(" ",2);
					mv = Integer.parseInt(tArr[1]);
					if(tArr.length > 2)
						adjList = tArr[2]; 
				}
				else{
					if(Integer.parseInt(tArr[1]) == -1){
						failedMatchNode = tmp;
						context.getCounter(failureCounter.failedMatches).increment(1);
					}
					else{
						count++;
						if(tmp < minNodeId){
							sb.append(":").append(minNodeId);
							//nodesList.add(minNodeId);
							minNodeId = tmp;
						}
						else{
							//nodesList.add(tmp);
							sb.append(":").append(tmp);
						}
					}
				}
			}
			//now check if this node is already matched or not.
			//if mv == -1 then failedMatchNode must be -1,implies not yet matched node
			//if mv != -1 then failedMatchNode can be either -1 or some positive value
				// if failedMatchNode == -1 then it implies successful match
				// if failedMatchNode != -1 and failedMatchNode == mv then it implies unsuccessful match
			if(mv == failedMatchNode){
				//not yet matched or match unsuccessful
				//mv == -1 implies not yet matched
				//mv != -1 implies match unsuccessful
				//StringBuffer sb = new StringBuffer();
				if(minNodeId < Integer.MAX_VALUE){
					//new match found here
					//need to emit the rejected nodes for synchronization in the next iteration. 
					mv = minNodeId;
					outputValue.set(Integer.toString(color) + ":" + Integer.toString(mv));
					context.write(key, outputValue);
					//nodesList.remove(0);	//remove Integer.MAX_VALUE from the nodesList.
					if(count > 1){
					sb.deleteCharAt(0);		// delete first :
					sb.delete(0,sb.indexOf(":") + 1);	// delete the first value since it is Integer.MAX_VALUE
					//for(int id : nodesList){
					//	sb.append(":").append(id);
					//}
					//sb.deleteCharAt(0);
					outputValue.set(sb.toString());
					context.write(key, outputValue);
					}
					//increment counter to indicate number of nodes matched till now.
					context.getCounter(matchingCounter.matchedNodes).increment(2);
				}
				else{
					if(!adjList.isEmpty()){
						outputValue.set(Integer.toString(color) + ":" + Integer.toString(-1) + ":" + adjList);
					}
					else{
						outputValue.set(Integer.toString(color) + ":" + Integer.toString(-1));
					}
					context.write(key,outputValue);	
				}
			}
			else{
				//already matched
				outputValue.set(Integer.toString(color) + ":" + Integer.toString(mv));
				context.write(key, outputValue);
				//need to emit the rejected nodes for synchronization in the next iteration.
				if(count > 1){
				sb.deleteCharAt(0);		// delete first :
				sb.delete(0,sb.indexOf(":") + 1);	// delete the first value since it is Integer.MAX_VALUE
				sb.append(":").append(minNodeId);	//add minNodeId
				outputValue.set(sb.toString());
				context.write(key, outputValue);
				}
				else if(count == 1){
					outputValue.set(Integer.toString(minNodeId));
					context.write(key,outputValue);
				}
			}
		}
	}

	protected int CURRENT_COLOR_VALUE;
	protected Path adjListPath;
	protected Path nodeColorsPath;
	protected Path MTempPath = new Path("mtmp");
	protected Path outputPath;
	protected int NUM_REDUCERS;
	
	@Override
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		if(args.length != 5){
			return -1;
		}
		
		int iter;
		int matchedNodes = 0;
		adjListPath = new Path(args[0]);
		nodeColorsPath = new Path(args[1]);
		outputPath = new Path(args[2]);
		NUM_REDUCERS = Integer.parseInt(args[3]);
		int MAX_ITERATIONS = Integer.parseInt(args[4]) + 1;			// plus one to allow for synchronization of the last iteration
		
		CURRENT_COLOR_VALUE = -((int)'a'); 

		FileSystem fs = FileSystem.get(getConf());
		
		Job job1 = Configure1();
		job1.waitForCompletion(true);

		for(iter = 0;iter < MAX_ITERATIONS;iter++){
		//for(iter = 0;iter < 1;iter++){
			Job job2 = Configure2();
			job2.waitForCompletion(true);
			Counters counters = job2.getCounters();
			Counter counter1 = counters.findCounter(matchingCounter.matchedNodes);
			Counter counter2 = counters.findCounter(failureCounter.failedMatches);
			matchedNodes += (int)counter1.getValue();
			System.out.println("Number of nodes matched after " + (iter + 1) + " iterations:" + matchedNodes);
			System.out.println("Number of failed matches this iteration:" + counter2.getValue());
			CURRENT_COLOR_VALUE--;
			fs.delete(MTempPath, true);
			fs.rename(outputPath, MTempPath);
		}
		fs.rename(MTempPath, outputPath);
		return 0;
	}
	
	protected Job Configure1() throws Exception{
		Configuration conf = getConf();
		conf.setInt("CURRENT_COLOR_VALUE", CURRENT_COLOR_VALUE);
		Job job = new Job(conf,"Matching1");
		job.setJarByClass(Matching.class);
		job.setMapperClass(MatchingMapper1.class);
		job.setReducerClass(MatchingReducer1.class);
		job.setNumReduceTasks(NUM_REDUCERS);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);
		job.setInputFormatClass(KeyValueTextInputFormat.class);
		FileInputFormat.addInputPath(job, adjListPath);
		FileInputFormat.addInputPath(job, nodeColorsPath);
		FileOutputFormat.setOutputPath(job,MTempPath);
		return job;
	}
	
	protected Job Configure2() throws Exception{
		Configuration conf = getConf();
		conf.setInt("CURRENT_COLOR_VALUE", CURRENT_COLOR_VALUE);
		Job job = new Job(conf,"Matching2");
		job.setJarByClass(Matching.class);
		job.setMapperClass(MatchingMapper2.class);
		job.setReducerClass(MatchingReducer2.class);
//		job.setNumReduceTasks(0);
		job.setNumReduceTasks(NUM_REDUCERS);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);
		job.setInputFormatClass(KeyValueTextInputFormat.class);
		FileInputFormat.addInputPath(job, MTempPath);
		FileOutputFormat.setOutputPath(job, outputPath);
		return job;
	}
}