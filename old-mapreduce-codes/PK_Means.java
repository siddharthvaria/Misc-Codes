//java 
import java.io.*;
import java.util.*;
import java.text.*;
//hadoop
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class PK_Means extends Configured implements Tool 
{
	protected static enum PK_MeansCounter { CONVERGE_CHECK }
	
	protected static ArrayList<String> centroids;
	////////////////////////////
	//First stage: compute the initial centroids
	//input: data file
	//output: centroids
	////////////////////////////
	public static class MapStage1 extends MapReduceBase implements Mapper<LongWritable, Text, IntWritable, Text>
	{
		int K = 0;
		Random randomGenerator = new Random();
		
		public void configure(JobConf job) {
			K = Integer.parseInt(job.get("K"));
			System.out.println("MapStage1 : K = " + K);
		}

		public void map(LongWritable key, Text value,OutputCollector<IntWritable, Text>output, Reporter reporter) throws IOException
		{
			int randomInt;
			String valueStr = value.toString();
			if (valueStr.startsWith("#"))		// ignore comment in the input file
				return;
			randomInt = randomGenerator.nextInt(K);
			output.collect(new IntWritable(randomInt), new Text(value));
		}  
	}
	public static class CombineStage1 extends MapReduceBase implements Reducer<IntWritable, Text, IntWritable, Text>
	{
		int dim = 0;

		public void configure(JobConf job)
		{
			dim = Integer.parseInt(job.get("dim"));
			System.out.println("CombineStage1 : dim = " + dim);
		}
		public void reduce(IntWritable key, Iterator<Text> values, OutputCollector<IntWritable, Text>output, Reporter reporter) throws IOException
		{
			double [] partialSum = new double[dim];
			String partialSumStr;
			int numPts = 0;
			int i;
			while(values.hasNext())
			{
				String valueStr = values.next().toString();
				String[] valueStrArray = valueStr.split("\t");
				for (i=0;i<dim;i++)
				{
					partialSum[i] += Double.parseDouble(valueStrArray[i+1]);	// ignore 0th index, its instance id
				}
				numPts++;
			}
			partialSumStr = Integer.toString(numPts);
			for (i=0;i<dim;i++)
			{
				partialSumStr = partialSumStr + "\t" + Double.toString(partialSum[i]);
			}
			output.collect( key, new Text(partialSumStr));
		}		
	}
	public static class RedStage1 extends MapReduceBase implements Reducer<IntWritable, Text, IntWritable, Text>
	{
		int dim = 0;

		public void configure(JobConf job)
		{
			dim = Integer.parseInt(job.get("dim"));
			System.out.println("RedStage1 : dim = " + dim);
		}
		public void reduce(IntWritable key, Iterator<Text> values, OutputCollector<IntWritable, Text>output, Reporter reporter) throws IOException
		{
			double [] sum = new double[dim];
			String sumStr;
			int numPts = 0;
			int i;
			while(values.hasNext())
			{
				String valueStr = values.next().toString();
				String[] valueArray = valueStr.split("\t");
				numPts += Integer.parseInt(valueArray[0]);
				for (i=0;i<dim;i++)
				{
					sum[i] += Double.parseDouble(valueArray[i+1]);
				}
			}
			sum[0] = sum[0] / numPts;
			sumStr = Double.toString(sum[0]);
			for (i=1;i<dim;i++)
			{
				sum[i] = sum[i] / numPts;
				sumStr = sumStr + "\t" + Double.toString(sum[i]);
			}
			centroids.set(key.get(),sumStr);
			output.collect( key, new Text("c\t" + sumStr));
		}
	}
	public static class MapStage2 extends MapReduceBase implements Mapper<LongWritable, Text, IntWritable, Text>
	{
		int K = 0;
		int dim = 0;
		public void configure(JobConf job) 
		{
			K = Integer.parseInt(job.get("K"));
			dim = Integer.parseInt(job.get("dim"));
			System.out.println("MapStage2 : K = " + K + ",dim = " + dim);
		}
		public void map(LongWritable key, Text value, OutputCollector<IntWritable, Text>output, Reporter reporter) throws IOException
		{
			int i,j;
			int clusterIndex = -1;
			double dist = 0.0;
			double minDist = Double.MAX_VALUE;
//			String instanceId;
			String tempStr = value.toString();
			if(tempStr.startsWith("#"))
				return;
			String[] valueStr = tempStr.split("\t");
//			instanceId = "i" + valueStr[0];
			for(i=0;i<K;i++)
			{	
				dist=0.0;
				String[] centroidValueStr = centroids.get(i).split("\t");
				for(j=0;j<dim;j++){
					dist += Math.pow(Double.parseDouble(valueStr[j+1]) - Double.parseDouble(centroidValueStr[j]),2);
				}
				dist = Math.sqrt(dist);
				if(dist < minDist)
				{
					minDist = dist;
					clusterIndex = i;
				}	
			}
			output.collect(new IntWritable(clusterIndex), new Text("i" + valueStr[0])); 
			output.collect(new IntWritable(clusterIndex), new Text(value));
		}
	}
	public static class CombineStage2 extends MapReduceBase implements Reducer<IntWritable, Text, IntWritable, Text>
	{
		int dim = 0;

		public void configure(JobConf job)
		{
			dim = Integer.parseInt(job.get("dim"));
			System.out.println("CombineStage2 : dim = " + dim);
		}
		public void reduce(IntWritable key, Iterator<Text> values, OutputCollector<IntWritable, Text>output, Reporter reporter) throws IOException
		{
			double [] partialSum = new double[dim];
			String partialSumStr;
			String instanceIdsStr="i";
			int numPts = 0;
			int i;
			while(values.hasNext())
			{
				String valueStr = values.next().toString();
				if (valueStr.charAt(0) == 'i')
					instanceIdsStr += "\t" + valueStr.substring(1);
				else
				{
				String[] valueStrArray = valueStr.split("\t");
				for (i=0;i<dim;i++)
				{
					partialSum[i] += Double.parseDouble(valueStrArray[i+1]);
				}
				numPts++;
				}
			}
			partialSumStr = Integer.toString(numPts);
			for (i=0;i<dim;i++){
				partialSumStr = partialSumStr + "\t" + Double.toString(partialSum[i]);
			}
			output.collect( key, new Text(partialSumStr));
			output.collect( key, new Text(instanceIdsStr));
		}	
	}
	public static class RedStage2 extends MapReduceBase implements Reducer<IntWritable, Text, IntWritable, Text>
	{
		int dim = 0;
		boolean change_reported = false;
		double converge_threshold = 0.0;
		public void configure(JobConf job)
		{
			dim = Integer.parseInt(job.get("dim"));
			converge_threshold = Double.parseDouble(job.get("converge_threshold"));
			System.out.println("RedStage2 : dim = " + dim + ",converge_threshold = " + converge_threshold);
		}
		public void reduce(IntWritable key, Iterator<Text> values, OutputCollector<IntWritable,Text>output, Reporter reporter) throws IOException
		{
			double [] sum = new double[dim];
			String [] prev_centroid;
			String sumStr;
			String instanceIdsStr="i";
			double dist=0.0;
			int numPts = 0;
			int i;
			while(values.hasNext())
			{
				String valueStr = values.next().toString();
				if(valueStr.charAt(0) == 'i')
					instanceIdsStr += valueStr.substring(1);
				else{
				String[] valueStrArray = valueStr.split("\t");
				numPts += Integer.parseInt(valueStrArray[0]);
				for (i=0;i<dim;i++)
				{
					sum[i] += Double.parseDouble(valueStrArray[i+1]);
				}
				}
			}
			sum[0] = sum[0] / numPts;
			sumStr = Double.toString(sum[0]);
			for (i=1;i<dim;i++)
			{
				sum[i] = sum[i] / numPts;
				sumStr = sumStr + "\t" + Double.toString(sum[i]);
			}

			prev_centroid = centroids.get(key.get()).split("\t");
			centroids.set(key.get(),sumStr);

			output.collect( key, new Text("c\t" + sumStr));
			output.collect( key, new Text(instanceIdsStr));
			
			//compare prev_centroid and new centroid to check for convergence
			if(!change_reported){
			for(i=0;i<dim;i++)
				dist += Math.pow(Double.parseDouble(prev_centroid[i])-sum[i],2);
			dist = Math.sqrt(dist);
			if(dist > converge_threshold) {
				reporter.incrCounter(PK_MeansCounter.CONVERGE_CHECK, 1);
				change_reported = true;
			}
			}
		}
	}

	//////////////////////////////////////////////////////////
	//		command line arguments			//	
	//////////////////////////////////////////////////////////

	protected int dim;
	protected int niterations=10;
	protected int K;
	protected int nreducers=1;
	protected static double converge_threshold = 0.01;

	protected Path input_path = null;
	protected Path kmeans_centroid_path = null;
	protected Path output_path = null;
//	protected Path temp_path = null;
	
	// command: PK_Means input_path output_path #dimensions #iterations #K #reducers #threshold
	// main function	
	public static void main (String[] args) throws Exception
	{
		int result = ToolRunner.run(new Configuration(), new PK_Means(), args);
		System.exit(result);
	}
	public int run (String[] args) throws Exception
	{
		if (args.length != 7)
	{
		System.out.println("Command Usage: PK_Means input_path output_path #dimensions #iterations #K #reducers #threshold\n");
		return -1;
	}
		int i;
		dim = Integer.parseInt(args[2]);
		niterations = Integer.parseInt(args[3]);
		K = Integer.parseInt(args[4]);
		nreducers = Integer.parseInt(args[5]);
		converge_threshold = Double.parseDouble(args[6]);
		input_path = new Path(args[0]);
		kmeans_centroid_path = new Path("kmeans_centroids");
		output_path = new Path(args[1]);
		centroids = new ArrayList<String>(K);
		for(i=0;i<K;i++)
			centroids.add(i,"");
		FileSystem fs = FileSystem.get(getConf());
		JobClient.runJob(configStage1());
		System.out.println("PK_Means : completed first stage of map reduce => initial centroids calculated\n");
		for(i=0;i<niterations;i++)
		{
			RunningJob job = JobClient.runJob(configStage2());
			Counters c = job.getCounters();
			long changed = c.getCounter(PK_MeansCounter.CONVERGE_CHECK);
			System.out.println("iteration:" + i);
			if(changed == 0){
				System.out.println("Parallel K_Means Clustering converged");
				fs.delete(kmeans_centroid_path);
				fs.rename(output_path,kmeans_centroid_path);
				break;
			}
			if(i == niterations){
				System.out.println("Maximum number of iterations reached");
				fs.delete(kmeans_centroid_path);
				fs.rename(output_path,kmeans_centroid_path);
				break;
			}	
			fs.delete(kmeans_centroid_path);
			fs.rename(output_path,kmeans_centroid_path);	
		}
		return 0;
	}
	protected JobConf configStage1() throws Exception
	{
		JobConf conf = new JobConf(getConf(), PK_Means.class);
		conf.set("K", Integer.toString(K));
		conf.set("dim",Integer.toString(dim));
		conf.setJobName("PK_Means_Stage1");
		
		conf.setMapperClass(MapStage1.class);
		conf.setCombinerClass(CombineStage1.class);
		conf.setReducerClass(RedStage1.class);

		FileInputFormat.setInputPaths(conf, input_path);  
		FileOutputFormat.setOutputPath(conf, kmeans_centroid_path);

		conf.setNumReduceTasks( nreducers );

		conf.setOutputKeyClass(IntWritable.class);
		conf.setOutputValueClass(Text.class);
  
		return conf;
	}
	protected JobConf configStage2() throws Exception
	{
		JobConf conf = new JobConf(getConf(), PK_Means.class);
		conf.set("K", Integer.toString(K));
		conf.set("dim",Integer.toString(dim));
		conf.set("converge_threshold",Double.toString(converge_threshold));
		conf.setJobName("PK_Means_Stage2");
		
		conf.setMapperClass(MapStage2.class);
		conf.setCombinerClass(CombineStage2.class);
		conf.setReducerClass(RedStage2.class);

		FileInputFormat.setInputPaths(conf, input_path);  
		FileOutputFormat.setOutputPath(conf, output_path);
	
		conf.setNumReduceTasks( nreducers );

		conf.setOutputKeyClass(IntWritable.class);
		conf.setOutputValueClass(Text.class);
		
		return conf;
	}
}
