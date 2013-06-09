import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Set;

import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.util.*;

class floatArrayWritable implements Writable{

	private float[] data;
	
	public floatArrayWritable(){
		data = null;
	}
	
	public float[] get(){
		return this.data;
	}
	public void set(float[] d){
		this.data = d;
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		//Deserialize the fields of this object from in.
		int len = in.readInt();
		this.data = new float[len];
		for(int i = 0;i < len;i++){
			data[i] = in.readFloat();
		}
	}

	@Override
	public void write(DataOutput out) throws IOException {
		//Serialize the fields of this object to out.
		int len = 0;
		if(data != null){
			len = data.length;
		}
		out.writeInt(len);
		
		for(int i = 0;i < len;i++){
			out.writeFloat(data[i]);
		}
	}
	
}
public class test extends Configured implements Tool{

	public static class map1 extends Mapper<LongWritable,Text,IntWritable,floatArrayWritable>{
		protected void setup(Context context) throws IOException, InterruptedException{
			System.out.println("### ### ### map1 ### ### ###");
		}
		protected void map(LongWritable k,Text v,Context context) throws IOException, InterruptedException{
			String[] kv = v.toString().split("\t");
			IntWritable key = new IntWritable(Integer.parseInt(kv[0]));
			String[] stmp = kv[1].split(" ");
			float[] ftmp = new float[stmp.length];
			for(int i = 0;i < ftmp.length;i++){
				ftmp[i] = Float.parseFloat(stmp[i]);
			}
			floatArrayWritable faw = new floatArrayWritable();
			faw.set(ftmp);
			context.write(key, faw);
		}
	}
	public static class reduce1 extends Reducer<IntWritable,floatArrayWritable,IntWritable,Text>{
		Text outputValue = new Text();
		protected void setup(Context context) throws IOException, InterruptedException{
			System.out.println("### ### ### reduce1 ### ### ###");
		}
		protected void reduce(IntWritable k,Iterable<floatArrayWritable> values,Context context) throws IOException, InterruptedException{
			int i;
			float index;
			float[] v;
			HashMap<Float,Float> aggregate = new HashMap<Float,Float>();
			for(floatArrayWritable faw : values){
				v = faw.get();
				for(i = 0;i < v.length;i = i + 2){
					if(aggregate.containsKey(v[i])){
						aggregate.put(v[i], aggregate.get(v[i]) + v[i + 1]);
					}
					else
						aggregate.put(v[i], v[i + 1]);
				}
			}
//			FloatWritable[] output = new FloatWritable[aggregate.size() * 2];
			
			StringBuffer sb = new StringBuffer();
			Set<Float> keys = aggregate.keySet();
			Iterator<Float> itr = keys.iterator();
			i = 0;
			while(itr.hasNext()){
				index = itr.next();
//				output[i++] = new FloatWritable(index);
//				output[i++] = new FloatWritable(aggregate.get(index));
				sb.append(" ").append(index).append(" ").append(aggregate.get(index));
			}
//			FloatArrayWritable outputValue  = new FloatArrayWritable(output);
			
			sb.deleteCharAt(0);
			outputValue.set(sb.toString());
			context.write(k, outputValue);

		}
	}
	
	Path inputPath;
	Path outputPath;
	
	public static void main(String[] args) throws Exception {
		int ret = ToolRunner.run(new Configuration(),new test(),args);
		System.exit(ret);
	}

	@Override
	public int run(String[] args) throws Exception {
		inputPath = new Path(args[0]);
		outputPath = new Path(args[1]);
		Job job1 = configStage();
		job1.waitForCompletion(true);
		System.out.println("### ### ### job completed ### ### ###");
		return 0;
	}
	
	public Job configStage() throws Exception{
		Configuration conf = getConf();
		Job job = new Job(conf,"job1");
		job.setJarByClass(test.class);
		job.setMapperClass(map1.class);
		job.setReducerClass(reduce1.class);
		job.setNumReduceTasks(1);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(floatArrayWritable.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, inputPath);
		FileOutputFormat.setOutputPath(job, outputPath);
		return job;
	}
}
