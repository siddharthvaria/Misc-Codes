package org.clustering.unweighted.matching;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.clustering.utilities.IntArrayWritable;


public class Matching1 extends Configured implements Tool{

	/**
	 * @param args
	 */

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
			String valStr = value.toString();
			
			if(valStr.charAt(0) == '-'){
				//its a key value pair from colors file
				int index = valStr.indexOf(" ");
				if(index > 0){
					outputValue.set(valStr.substring(0,index));
					context.write(outputKey, outputValue);
				}
				else
					context.write(outputKey, value);
			}
			else{
				context.write(outputKey,value);
			}
		}
	}

	public static class MatchingReducer1 extends Reducer<IntWritable,Text,IntWritable,Text>{
		private Text outputValue = new Text();
		protected void setup(Context context) throws IOException,InterruptedException{
			System.out.println("### ### ### Matching Reducer1 ### ### ###");
		}
		protected void reduce(IntWritable key,Iterable<Text> values,Context context) throws IOException, InterruptedException{
			StringBuffer cAdj = new StringBuffer();
			for(Text t : values){
				if(t.toString().charAt(0) == '-'){
					cAdj.insert(0,t.toString() + ":" + Integer.toString(-1) + ":");
				}
				else{
					cAdj.append(t.toString());
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
			String[] valArr = value.toString().split(":");
			int color = Integer.parseInt(valArr[0]);
			int match = Integer.parseInt(valArr[1]);
			if(color == ccv){
				if(valArr.length == 3 && valArr[2].length() > 0 && match == -1){
					int index1 = valArr[2].indexOf(" ");
					match = Integer.parseInt(valArr[2].substring(0,index1));
					int index2 = valArr[2].indexOf(" ",index1 + 1);
					String weight = valArr[2].substring(index1 + 1,(index2 > 0) ? index2 : valArr[2].length());
					//emit <match,"key:weight">
					outputKey.set(match);
					outputValue.set(key.toString() + ":" + weight);
					context.write(outputKey, outputValue);
				}
				outputKey.set(k);
				outputValue.set(Integer.toString(color) + ":" + Integer.toString(match));
				context.write(outputKey, outputValue);
			}
			else{
				outputKey.set(k);
				context.write(outputKey, value);
			}
		}
	}
	
	public static class MatchingReducer2 extends Reducer<IntWritable,Text,IntWritable,Text>{
		private Text outputValue = new Text();
		protected void setup(Context context) throws IOException,	InterruptedException{
			System.out.println("### ### ### Matching Reducer2 ### ### ###");
		}
		protected void reduce(IntWritable key,Iterable<Text> values,Context context) throws IOException, InterruptedException{
			int maxWeight = 0;
			int mwNodeId = -1;
			int color = -1;
			int match = -1;
			String adjList = "";
			StringBuffer sb = new StringBuffer();
			for(Text t : values){
				String[] tArr = t.toString().split(":");
				int tmp1 = Integer.parseInt(tArr[0]);
				int tmp2 = Integer.parseInt(tArr[1]);
				if(tmp1 < 0){
					color = tmp1;
					match = tmp2;
					if(tArr.length == 3){
						adjList = tArr[2];
					}
				}
				else{
					if(tmp2 > maxWeight){
						sb.append(":").append(mwNodeId);
						maxWeight = tmp2;
						mwNodeId = tmp1;
					}
					else{
						sb.append(":").append(tmp1);
					}
				}
			}
			if(match == -1){
				if(mwNodeId >= 0){
					//context.getCounter(matchingCounter.matchedNodes).increment(2);
					match = mwNodeId;
					//delete first three chars ":-1"
					sb.delete(0,3);
					if(sb.length() > 0){
						sb.deleteCharAt(0);
						outputValue.set(sb.toString());
						context.write(key, outputValue);
					}
					outputValue.set(Integer.toString(color) + ":" + Integer.toString(match));
					context.write(key, outputValue);
				}
				else{
					//sb is empty
					sb.append(color).append(":").append(match);
					if(adjList.length() > 0){
						sb.append(":").append(adjList);
					}
					outputValue.set(sb.toString());
					context.write(key, outputValue);
				}
			}
			else{
				outputValue.set(Integer.toString(color) + ":" + Integer.toString(match));
				context.write(key, outputValue);
				if(mwNodeId >= 0){
					//delete first three chars ":-1"
					sb.delete(0, 3);
					sb.insert(0,mwNodeId);
					outputValue.set(sb.toString());
					context.write(key, outputValue);
				}
			}
		}
	}

	public static class MatchingMapper3 extends Mapper<Text,Text,IntWritable,IntArrayWritable>{
		private IntWritable outputKey = new IntWritable();
		private IntArrayWritable outputValue = new IntArrayWritable();
		protected void setup(Context context) throws IOException,	InterruptedException{
			System.out.println("### ### ### Matching Mapper3 ### ### ###");
		}
		protected void map(Text key,Text value,Context context) throws IOException, InterruptedException{
			int k = Integer.parseInt(key.toString());
			int i;
			int[] tmp = new int[2];
			String[] valArr = value.toString().split(":");
			int match;
			int color = Integer.parseInt(valArr[0]);
			if(color < 0){
				//if unmatched, inform neighbors about the status
				match = Integer.parseInt(valArr[1]);
				if(valArr.length == 3 && valArr[2].length() > 0 && match == -1){
					String[] adjList = valArr[2].split(" ");
					tmp[0] = k;
					for(i = 0;i < adjList.length;i = i + 2){		//increment by 2 due to weights
						tmp[1] = Integer.parseInt(adjList[i + 1]);
						outputKey.set(Integer.parseInt(adjList[i]));
						outputValue.set(tmp);
						context.write(outputKey, outputValue);
					}
				}
				//emit color and match to self
				outputKey.set(k);
				tmp[0] = color;
				tmp[1] = match;
				outputValue.set(tmp);
				context.write(outputKey, outputValue);
			}
			else{
				//inform neighbors about failed matches
				tmp[0] = k;
				tmp[1] = -1;
				outputValue.set(tmp);
				for(i = 0;i < valArr.length;i++){
					outputKey.set(Integer.parseInt(valArr[i]));
					context.write(outputKey, outputValue);
				}
			}
		}
	}
	
	public static class MatchingReducer3 extends Reducer<IntWritable,IntArrayWritable,IntWritable,Text>{
		private Text outputValue = new Text();
		protected void setup(Context context) throws IOException,	InterruptedException{
			System.out.println("### ### ### Matching Reducer3 ### ### ###");
		}		
		protected void reduce(IntWritable key,Iterable<IntArrayWritable> values,Context context) throws IOException, InterruptedException{
			int failedMatch = -1;
			int[] tmp;
			int color = -1;
			int match = -1;
			int maxWeight = 0;
			int mxNodeId = -1;
			StringBuffer sb1 = new StringBuffer();
			for(IntArrayWritable iaw : values){
				tmp = iaw.get();
				if(tmp[0] < 0){
					//tmp[0] is a color value
					color = tmp[0];
					match = tmp[1];
				}
				else{
					//tmp[1] is a positive value or -1
					if(tmp[1] > 0){
						if(tmp[1] > maxWeight){
							sb1.append(" ").append(mxNodeId).append(" ").append(maxWeight);
							mxNodeId = tmp[0];
							maxWeight = tmp[1];
						}
						else{
							sb1.append(" ").append(tmp[0]).append(" ").append(tmp[1]);
						}
					}
					else{
						failedMatch = tmp[0];
						//context.getCounter(failureCounter.failedMatches).increment(1);
					}
				}
			}
			
			StringBuffer sb2 = new StringBuffer();
			if(match == failedMatch){
				match = -1;
				//append color and match
				sb2.append(color).append(":").append(match);
				if(mxNodeId >= 0){
					//delete the first five chars " -1 0" from sb1
					sb1.delete(0, 5);
					//now append mxNodeId and maxWeight
					sb2.append(":").append(mxNodeId).append(" ").append(maxWeight);
					sb2.append(sb1);
				}
			}
			else{
				context.getCounter(matchingCounter.matchedNodes).increment(1);
				sb2.append(color).append(":").append(match);
			}
			outputValue.set(sb2.toString());
			context.write(key, outputValue);
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
		int MAX_ITERATIONS = Integer.parseInt(args[4]);	
		
		CURRENT_COLOR_VALUE = -((int)'a'); 

		FileSystem fs = FileSystem.get(getConf());
		
		Job job1 = Configure1();
		job1.waitForCompletion(true);

		for(iter = 0;iter < MAX_ITERATIONS;iter++){
		//for(iter = 0;iter < 1;iter++){
			Job job2 = Configure2();
			job2.waitForCompletion(true);
			//Counters counters1 = job2.getCounters();
			//matchedNodes += (int) counters1.findCounter(matchingCounter.matchedNodes).getValue();
			
			fs.delete(MTempPath, true);
			
			Job job3 = Configure3();
			job3.waitForCompletion(true);
			Counters counters2 = job3.getCounters();
			matchedNodes  =  (int) counters2.findCounter(matchingCounter.matchedNodes).getValue();
			System.out.println("Number of nodes matched after " + (iter + 1) + " iterations:" + matchedNodes);
			CURRENT_COLOR_VALUE--;
	
			fs.delete(outputPath, true);
		}
		fs.rename(MTempPath, outputPath);
		return 0;
	}
	
	protected Job Configure1() throws Exception{
		Configuration conf = getConf();
		Job job = new Job(conf,"Matching1");
		job.setJarByClass(Matching1.class);
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
		job.setJarByClass(Matching1.class);
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
	
	protected Job Configure3() throws Exception{
		Configuration conf = getConf();
		Job job = new Job(conf,"Matching3");
		job.setJarByClass(Matching1.class);
		job.setMapperClass(MatchingMapper3.class);
		job.setReducerClass(MatchingReducer3.class);
//		job.setNumReduceTasks(0);
		job.setNumReduceTasks(NUM_REDUCERS);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(IntArrayWritable.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);
		job.setInputFormatClass(KeyValueTextInputFormat.class);
		FileInputFormat.addInputPath(job, outputPath);
		FileOutputFormat.setOutputPath(job, MTempPath);
		return job;
	}		
}