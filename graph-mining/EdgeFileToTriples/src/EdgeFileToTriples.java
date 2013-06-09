import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;

//import org.apache.hadoop.mapreduce.lib.*;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;


class FloatArrayListWritable implements Writable{

	private ArrayList<Float> al;
	public FloatArrayListWritable(){
		al = null;
	}
	
	public void set(ArrayList<Float> tmp){
		al = tmp;
	}
	public ArrayList<Float> get(){
		return al;
	}
	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		int len = in.readInt();
		al = new ArrayList<Float>(len);
		for(int i = 0;i < len;i++){
			al.add(in.readFloat());
		}
	}

	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		int len = al.size();
		out.writeInt(len);
		for(int i = 0;i < len;i++){
			out.writeFloat(al.get(i));
		}
	}	
}

public class EdgeFileToTriples extends Configured implements Tool{
	
	public static class map1ForUnweighted extends Mapper<LongWritable,Text,IntWritable,Text>{
		IntWritable oKey = new IntWritable();
		Text oValue = new Text();
		float one = 1;
		protected void setup(Context context)throws IOException, InterruptedException{
			System.out.println("############ map1ForUnweighted ############");
		}
		protected void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException{
			
			if(value.charAt(0) == '#')		//ignore comments
				return;
			String[] kv = value.toString().split("\t");
			int u = Integer.parseInt(kv[0]) + 1;
			int v = Integer.parseInt(kv[1]) + 1;
			oKey.set(v);
			oValue.set(u + "\t" + one);
			context.write(oKey, oValue);
		}
	}
	
	public static class map1ForWeighted extends Mapper<LongWritable,Text,IntWritable,Text>{
		IntWritable oKey = new IntWritable();
		Text oValue = new Text();
		protected void setup(Context context)throws IOException, InterruptedException{
			System.out.println("############ map1ForWeighted ############");
		}
		protected void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException{

			if(value.charAt(0) == '#')		//ignore comments
				return;

			String[] kv = value.toString().split("\t");
			int u = Integer.parseInt(kv[0]) + 1;
			int v = Integer.parseInt(kv[1]) + 1;
			oKey.set(v);
			oValue.set(u + "\t" + kv[2]);
			context.write(oKey, oValue);
		}
	}
		
	public static class reduce1 extends Reducer<IntWritable,Text,IntWritable,Text>{
		Text oValue = new Text();
		float one = 1;
		protected void setup(Context context)throws IOException, InterruptedException{
			System.out.println("############ reduce1 ############");
		}
		
		protected void reduce(IntWritable key,Iterable<Text> values,Context context) throws IOException, InterruptedException{
			
			for(Text val : values){
				context.write(key,val);
			}
			//add self loop
			String tmp = key.toString() + "\t" + Float.toString(one);
			oValue.set(tmp);
			context.write(key,oValue);
		}
	}


	public static class map2ForUnweighted extends Mapper<LongWritable,Text,IntWritable,FloatArrayListWritable>{
		IntWritable oKey = new IntWritable();
		FloatArrayListWritable faw = new FloatArrayListWritable();
		float one = 1;
	
		protected void setup(Context context)throws IOException, InterruptedException{
			System.out.println("############ map2ForUnweighted ############");
		}
		protected void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException{

			if(value.charAt(0) == '#')		//ignore comments
				return;

			String[] kv = value.toString().split("\t");
			int u = Integer.parseInt(kv[0]) + 1;
			float v = Integer.parseInt(kv[1]) + 1;
			ArrayList<Float> al = new ArrayList<Float>();
			
			al.add(v);
			al.add(one);
			
			
			oKey.set(u);
			faw.set(al);
			context.write(oKey, faw);
		}
		
	}
	
	public static class map2ForWeighted extends Mapper<LongWritable,Text,IntWritable,FloatArrayListWritable>{
		IntWritable oKey = new IntWritable();
		FloatArrayListWritable faw = new FloatArrayListWritable();
	
		protected void setup(Context context)throws IOException, InterruptedException{
			System.out.println("############ map2ForWeighted ############");
		}
		protected void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException{
			
			if(value.charAt(0) == '#')		//ignore comments
				return;

			String[] kv = value.toString().split("\t");
			int u = Integer.parseInt(kv[0]) + 1;
			float v = Integer.parseInt(kv[1]) + 1;
			ArrayList<Float> al = new ArrayList<Float>();
			
			al.add(v);
			al.add(Float.parseFloat(kv[2]));
			
			
			oKey.set(u);
			faw.set(al);
			context.write(oKey, faw);
		}
		
	}

	
	public static class combine2 extends Reducer<IntWritable,FloatArrayListWritable,IntWritable,FloatArrayListWritable>{
		FloatArrayListWritable oKey = new FloatArrayListWritable();
		protected void setup(Context context)throws IOException, InterruptedException{
			System.out.println("############ combine2 ############");
		}

		protected void reduce(IntWritable key,Iterable<FloatArrayListWritable> values,Context context) throws IOException, InterruptedException{
			ArrayList<Float> global = new ArrayList<Float>();
			ArrayList<Float> local = new ArrayList<Float>();
			
			for(FloatArrayListWritable faw : values){
				local = faw.get();
				global.add(local.get(0));
				global.add(local.get(1));				
			}
			oKey.set(global);
			context.write(key, oKey);
		}
		
	}
	
	
	public static class reduce2 extends Reducer<IntWritable,FloatArrayListWritable,IntWritable,Text>{
		IntWritable oKey = new IntWritable();
		Text oValue = new Text();
		protected void setup(Context context)throws IOException, InterruptedException{
			System.out.println("############ reduce2 ############");
		}

		protected void reduce(IntWritable key,Iterable<FloatArrayListWritable> values,Context context) throws IOException, InterruptedException{
			ArrayList<Float> global = new ArrayList<Float>();
			ArrayList<Float> local = new ArrayList<Float>();
			for(FloatArrayListWritable faw : values){
				local = faw.get();
				for(float v : local){
					global.add(v);
				}
			}
			int i;
			float sum = 1;
			
			for(i = 1;i < global.size();i = i + 2){
				sum += global.get(i);
			}
			
			
			for(i = 0;i < global.size();i = i + 2){
				StringBuffer sb = new StringBuffer();
				sb.append(key.get()).append("\t").append(global.get(i + 1) / sum);				
				oKey.set(Math.round(global.get(i)));
				oValue.set(sb.toString());
				context.write(oKey, oValue);
			}
			String tmp = key.toString() + "\t" + Float.toString(1 / sum);
			oValue.set(tmp);
			context.write(key, oValue);
		}
	}
	
	
	protected Path inputPath;
	protected Path MPath;
	protected Path MgPath;
	protected int numReducers;
	protected char graphType;
	
	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		int ret = ToolRunner.run(new Configuration(),new EdgeFileToTriples(), args);
		System.exit(ret);
	}

	@Override
	public int run(String[] args) throws Exception {
		if(args.length != 5){
			System.out.println("Usage: EdgeFileToTriples inputPath MPath MgPath numReducers graphType");
			return -1;
		}
		// TODO Auto-generated method stub
		inputPath = new Path(args[0]);
		MPath = new Path(args[1]);
		MgPath = new Path(args[2]);
		numReducers = Integer.parseInt(args[3]);
		graphType = args[4].charAt(0);
		Job job1 = ConfigStage1(graphType);
		job1.waitForCompletion(true);
		//Job job2 = ConfigStage2(graphType);
		//job2.waitForCompletion(true);
		return 0;
	}
	
	public Job ConfigStage1(char gType) throws Exception{
		Configuration conf = getConf();
		Job job = new Job(conf,"JOB1");
		job.setJarByClass(EdgeFileToTriples.class);
		if(gType == 'u')
			job.setMapperClass(map1ForUnweighted.class);
		else
			job.setMapperClass(map1ForWeighted.class);
		job.setReducerClass(reduce1.class);
		job.setNumReduceTasks(numReducers);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, inputPath);
		FileOutputFormat.setOutputPath(job, MPath);
		return job;
	}
	public Job ConfigStage2(char gType) throws Exception{
		Configuration conf = getConf();
		Job job = new Job(conf,"JOB2");
		job.setJarByClass(EdgeFileToTriples.class);
		if(gType == 'u')
			job.setMapperClass(map2ForUnweighted.class);
		else
			job.setMapperClass(map2ForWeighted.class);
//		job.setCombinerClass(combine2.class);
		job.setReducerClass(reduce2.class);
		job.setNumReduceTasks(numReducers);
		job.setOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(FloatArrayListWritable.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, inputPath);
		FileOutputFormat.setOutputPath(job, MgPath);
		return job;
	}
}
