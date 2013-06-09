
 //hadoop 
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import java.io.*;
import java.util.ArrayList;
import java.util.List;

class Node {
	public static enum Color{
		  WHITE,GRAY,BLACK
	}
	private String id;
	private Color color;
	private List<String> adjList;
	private int dist;
	private String Parent;
	public Node(){
		id = null;
		color = Color.WHITE;
		adjList = new ArrayList<String>();
		dist = Integer.MAX_VALUE;
		Parent = null;
	}
	public Node(String info){
		String[] infoArray = info.split("\t");
		setId(infoArray[0]);
		String[] valueArray = infoArray[1].split("\\|");
		String[] adjListArray = valueArray[0].split(" ");
		for(int i = 0;i < adjListArray.length;i++)
			adjList.add(adjListArray[i]);
		if(valueArray[1].equals("WHITE"))
			color = Color.WHITE;
		else if(valueArray[1].equals("GRAY"))
			color = Color.GRAY;
		else
			color = Color.BLACK;
		dist = Integer.parseInt(valueArray[2]);	
		Parent = valueArray[3];
	}
	public String getInfo(){
		StringBuffer sb = new StringBuffer();
		if(adjList.size() > 0){
			sb.append(adjList.get(0));
			for(int i = 1;i < adjList.size();i++)
				sb.append(" ").append(adjList.get(i));
		}
		else
			sb.append("");
		sb.append("|").append(color.toString());
		sb.append("|").append(Integer.toString(dist));
		sb.append("|").append(Parent);
		return(sb.toString());
	}
	public void setDist(int dist) {
		this.dist = dist;
	}
	public int getDist() {
		return dist;
	}
	public void setParent(String parent) {
		Parent = parent;
	}
	public String getParent() {
		return Parent;
	}
	public void setColor(Color color) {
		this.color = color;
	}
	public Color getColor() {
		return color;
	}
	public void setId(String id) {
		this.id = id;
	}
	public String getId() {
		return id;
	}
	public List<String> getAdjList(){
		return adjList;
	}
	public void setAdjList(List<String> list){
		this.adjList = list;
	}
}

public class PBFS extends Configured implements Tool {
	protected static enum bfsCounter{
		convergeCheck
	} 
	public static class MapStage1 extends Mapper<LongWritable,Text,Text,Text>{
		private String startNode;
		protected void setup (Context context) throws IOException, InterruptedException{
			Configuration conf = context.getConfiguration();
			startNode = conf.get("startNode");
		}
		public void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException{
			String str = value.toString();
			String[] strArray = str.split("\t");
			if(strArray[0].equals(startNode))
				context.write(new Text(strArray[0]),new Text(strArray[1] + "|" + "GRAY" + "|" + "0" + "|" + "null" ));
			else
				context.write(new Text(strArray[0]),new Text(strArray[1] + "|" + "WHITE" + "|" + "MAX" + "|" + "null" ));
		}
	}
	//no reducer for first stage of map reduce
	public static class MapStage2 extends Mapper<LongWritable,Text,Text,Text>{
		private String graphType;
		protected void setup(Context context) throws IOException, InterruptedException{
			Configuration conf = context.getConfiguration();
			graphType = conf.get("graphType");
		}
		public void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException{
			String str = value.toString();
			Node node = new Node(str);
			if(graphType.equals("u")){
				if(node.getColor() == Node.Color.GRAY){
					for(String s: node.getAdjList()){
						Node adjNode = new Node();
						adjNode.setId(s);
						adjNode.setColor(Node.Color.GRAY);
						adjNode.setParent(node.getId());
						adjNode.setDist(node.getDist() + 1);
						context.write(new Text(s),new Text(adjNode.getInfo()));
					}
					node.setColor(Node.Color.BLACK);
					context.write(new Text(node.getId()),new Text(node.getInfo()));
				}
				else
					context.write(new Text(node.getId()),new Text(node.getInfo()));
			}
			else{
				if(node.getColor() == Node.Color.GRAY){
					for(String s: node.getAdjList()){
						Node adjNode = new Node();
						adjNode.setId(s);
						adjNode.setColor(Node.Color.GRAY);
						adjNode.setParent(node.getId());
						adjNode.setDist(node.getDist() + Integer.parseInt(s.split(",")[1]));
						context.write(new Text(s.split(",")[0]),new Text(adjNode.getInfo()));
					}
					node.setColor(Node.Color.BLACK);
					context.write(new Text(node.getId()),new Text(node.getInfo()));
				}
				else
					context.write(new Text(node.getId()),new Text(node.getInfo()));
			}
		}
	}
	public static class ReduceStage2 extends Reducer<Text,Text,Text,Text>{
		protected void setup(Context context) throws IOException, InterruptedException{
			Configuration conf = context.getConfiguration();
			conf.get("graphType");
		}
		public void reduce(Text key,Iterable<Text> values,Context context) throws IOException, InterruptedException{
			Node node = new Node();
			node.setId(key.toString());
			for(Text val:values){
				Node tmpNode = new Node(key.toString() + "\t" + val.toString());
				if(node.getAdjList().size() == 0 && tmpNode.getAdjList().size() > 0){
					node.setAdjList(tmpNode.getAdjList());
				}
				if(node.getColor().ordinal() < tmpNode.getColor().ordinal())
					node.setColor(tmpNode.getColor());
				if(node.getDist() > tmpNode.getDist() && node.getColor() != Node.Color.BLACK){
					node.setDist(tmpNode.getDist());
					node.setParent(tmpNode.getParent());
				}
			}
			context.write(new Text(node.getId()),new Text(node.getInfo()));
			if(node.getColor() == Node.Color.GRAY)
					context.getCounter(bfsCounter.convergeCheck).increment(1);
			}
		}
	
	
	
	///////////////////////////
	//command line parameters//
	///////////////////////////
	
	// PBFS inputPath outputPath graphType startNode numReducers
	protected Path tmpPath = null;
	protected Path inputPath = null;
	protected Path outputPath = null;
	protected String graphType;
	protected String startNode;
	protected int nReducers;
	
	public int run(String[] args) throws Exception {
		
		tmpPath = new Path("tmp");
		inputPath = new Path(args[0]);
		outputPath = new Path(args[1]);
		graphType = args[2];
		startNode = args[3];
		nReducers = Integer.parseInt(args[4]);
		
		FileSystem fs = FileSystem.get(getConf());
		//first stage of map reduce
		Job job1 = ConfigStage1();
		job1.waitForCompletion(true);
		fs.delete(inputPath,true);
		fs.rename(outputPath,inputPath);
		while(true){
			Job job2 = ConfigStage2();
			job2.waitForCompletion(true);
			Counters c = job2.getCounters();
			Counter counterValue = c.findCounter(bfsCounter.convergeCheck);
			if(counterValue.getValue() == 0){
				fs.delete(inputPath,true);
				fs.rename(outputPath,inputPath);
				break;
			}
			fs.delete(inputPath,true);
			fs.rename(outputPath,inputPath);
		}
		return 0;
	}
	public Job ConfigStage1() throws Exception{
		Configuration conf = getConf();
		conf.set("startNode",startNode);
		Job job = new Job(conf,"BFS_STAGE1");
		job.setJarByClass(PBFS.class);
		job.setMapperClass(MapStage1.class);
		job.setNumReduceTasks(0);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.setInputPaths(job,inputPath);  
		FileOutputFormat.setOutputPath(job,outputPath);  
		return job;
	}
	public Job ConfigStage2() throws Exception{
		Configuration conf = getConf();
		conf.set("graphType",graphType);
		Job job = new Job(conf,"BFS_STAGE2");
		job.setJarByClass(PBFS.class);
		job.setMapperClass(MapStage2.class);
		job.setReducerClass(ReduceStage2.class);
		job.setNumReduceTasks(nReducers);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.setInputPaths(job,inputPath);
		FileOutputFormat.setOutputPath(job,outputPath);
		return job;
	}
	public static void main(String[] args) throws Exception {
		int rv = ToolRunner.run(new Configuration(), new PBFS(),args);
		System.exit(rv);
	}
}