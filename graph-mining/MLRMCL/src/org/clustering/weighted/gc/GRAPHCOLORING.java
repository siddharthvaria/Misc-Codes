package org.clustering.weighted.gc;

import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.util.Tool;
import org.clustering.utilities.IntArrayWritable;

public class GRAPHCOLORING extends Configured implements Tool{

	protected static enum coloredNodesCounter{
		coloredNodes
	}

	public static class gcMapper extends Mapper<Text,Text,Text,IntArrayWritable>{
		protected void setup(Context context) throws IOException, InterruptedException{
			
		}
		protected void map(Text key,Text value,Context context) throws IOException, InterruptedException{
			
		}
	}
	
	public static class gcReducer extends Reducer<Text,IntArrayWritable,Text,Text>{
		protected void setup(Context context) throws IOException, InterruptedException{
			
		}
		protected void reduce(Text key,Iterable<IntArrayWritable> values,Context context) throws IOException, InterruptedException{
			
		}
	}
	@Override
	public int run(String[] arg0) throws Exception {
		// TODO Auto-generated method stub
		return 0;
	}

}
