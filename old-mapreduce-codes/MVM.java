import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.io.*;

public class MVM extends Configured implements Tool{
	public static class Mapper1 extends Mapper<LongWritable,Text,Text,Text>{
		private Text ov = new Text();
		private Text ok = new Text();
		protected void setup(Context context) throws IOException, InterruptedException{
			System.out.println("#################### Mapper1 ####################");
		}
		protected void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException{
			String[] tmp = value.toString().split("\t");
			ok.set(tmp[1]);
			ov.set(tmp[0] + ":" + tmp[2]);
			context.write(ok, ov);
		}
	}
	public static class Mapper2 extends Mapper<Text,Text,Text,Text>{
		protected void setup(Context context) throws IOException, InterruptedException{
			System.out.println("#################### Mapper2 ####################");
		}
		protected void map(Text key,Text value,Context context) throws IOException, InterruptedException{
			context.write(key, value);
		}
	}
	public static class Reducer2 extends Reducer<Text,Text,Text,Text>{
		private Text ov = new Text();
		private Text ok = new Text();
		protected void setup(Context context) throws IOException, InterruptedException{
			System.out.println("#################### Reducer2 ####################");
		}
		protected void reduce(Text key,Iterable<Text>values,Context context) throws IOException, InterruptedException{
			double multiplier = 0.0;
			Map<Integer,Double> hm = new HashMap<Integer,Double>();
			for(Text v:values){
				String[] tmp = v.toString().split(":");
				if(tmp.length == 1)
					multiplier = Double.parseDouble(tmp[0]);
				else{
					hm.put(Integer.parseInt(tmp[0]),Double.parseDouble(tmp[1]));
				}
			}
			if(multiplier != 0.0){
				
				for(Map.Entry<Integer,Double> entry : hm.entrySet()) {
				    ok.set(Integer.toString(entry.getKey()));
				    ov.set(Double.toString(entry.getValue() * multiplier));
				    context.write(ok, ov);
				}
			}
		}
	}
	public static class Mapper3 extends Mapper<Text,Text,Text,Text>{
		protected void setup(Context context) throws IOException, InterruptedException{
			System.out.println("#################### Mapper3 ####################");
		}
		protected void map(Text key,Text value,Context context) throws IOException, InterruptedException {
			context.write(key, value);
		}
	}
	
	public static class Reducer3 extends Reducer<Text,Text,Text,Text> {
		private Text ov = new Text();
		protected void setup(Context context) throws IOException, InterruptedException{
			System.out.println("#################### Reducer3 ####################");
		}
		protected void reduce(Text key,Iterable<Text> values,Context context) throws IOException, InterruptedException {
			double sum = 0.0;
			for(Text v:values){
				sum += Double.parseDouble(v.toString());
			}
			if(sum != 0.0){
			ov.set(Double.toString(sum));
			context.write(key, ov);
			}
		}
	}
	// initially ip contains the matrix files, vp contains the vector files
	protected Path tmp = new Path("tmp");
	protected Path op = null;
	protected Path ip = null;
	protected Path vp = null;
	protected int reducers;
	
	public static void main(String[] args) throws Exception {
		int rv = ToolRunner.run(new Configuration(),new MVM(), args);
		System.exit(rv);
	}

	@Override
	public int run(String[] args) throws Exception {
		if(args.length != 4)
			System.out.println("USAGE: MVM input_path output_path vector_path number_reducers");
		ip = new Path(args[0]);
		op = new Path(args[1]);
		vp = new Path(args[2]);
		reducers = Integer.parseInt(args[3]);
		FileSystem fs = FileSystem.get(getConf());
		
		Job job1 = ConfigStage1();
		job1.waitForCompletion(true);
		Job job2 = ConfigStage2();
		job2.waitForCompletion(true);
		fs.delete(ip,true);
		fs.rename(op,ip);
		Job job3 = ConfigStage3();
		job3.waitForCompletion(true);
		return 0;
	}
	private Job ConfigStage1() throws Exception{
		Configuration conf = getConf();
		Job job = new Job(conf,"");
		job.setJarByClass(MVM.class);
		job.setMapperClass(Mapper1.class);
		job.setNumReduceTasks(0);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job,ip);
		FileOutputFormat.setOutputPath(job,tmp);
		return job;
	}
	private Job ConfigStage2() throws Exception{
		Configuration conf = getConf();
		Job job = new Job(conf,"");
		job.setJarByClass(MVM.class);
		job.setMapperClass(Mapper2.class);
		job.setReducerClass(Reducer2.class);
		job.setNumReduceTasks(reducers);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setInputFormatClass(KeyValueTextInputFormat.class);
		FileInputFormat.setInputPaths(job,vp,tmp);
		FileOutputFormat.setOutputPath(job, op);
		return job;
	}
	private Job ConfigStage3() throws Exception {
		Configuration conf = getConf();
		Job job = new Job(conf,"");
		job.setJarByClass(MVM.class);
		job.setMapperClass(Mapper3.class);
		job.setCombinerClass(Reducer3.class);
		job.setReducerClass(Reducer3.class);
		job.setNumReduceTasks(reducers);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setInputFormatClass(KeyValueTextInputFormat.class);
		FileInputFormat.addInputPath(job, ip);
		FileOutputFormat.setOutputPath(job, op);
		return job;
	}
}
