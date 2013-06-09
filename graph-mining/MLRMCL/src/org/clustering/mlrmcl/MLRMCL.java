package org.clustering.mlrmcl;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

//import org.clustering.unweighted.gc.*;
//import org.clustering.unweighted.matching.*;
//import org.clustering.weighted.gc.*;
import org.clustering.utilities.*;

public class MLRMCL extends Configured implements Tool{

	/**
	 * @param args 
	 */
	
	
	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		int rv = ToolRunner.run(new MLRMCL(),args);
		
	}
	
	@Override
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		/*
		USAGE: MLRMCL --input="edge file path" --outputPath="final clusters path" --NUM_NODES=? --INFLATE_CONSTANT=?
		--MLRMCL_PRUNE_A=? --MLRMCL_PRUNE_B=? --isWeighted="Y or N" --NUM_REDUCERS=? --CONVERGE_THRESHOLD=?
		--MAX_ITERATIONS=?
		*/
		if(args.length != 10){
			System.out.println("USAGE: ");
			System.out.println("FOR EXAMPLE: ");
			return -1;
		}
		
		String edgeListPath = args[0].substring(args[0].indexOf('=') + 1);
		String adjListPath = "adjList";
		String nodeColorsPath = "nodeColors";
		String matchingPath = "matching";
		Path finalClustersPath = new Path(args[1].substring(args[1].indexOf('=') + 1));
		String NUM_NODES = args[2].substring(args[2].indexOf('=') + 1);
		float INFLATE_CONSTANT = Float.parseFloat(args[3].substring(args[3].indexOf('=') + 1));		
		float MLRMCL_PRUNE_A = Float.parseFloat(args[4].substring(args[4].indexOf('=') + 1));
		float MLRMCL_PRUNE_B = Float.parseFloat(args[5].substring(args[5].indexOf('=') + 1));
		String isWeighted = args[6].substring(args[6].indexOf('=') + 1);
		String NUM_REDUCERS = args[7].substring(args[7].indexOf('=') + 1);
		float CONVERGE_THRESHOLD = Float.parseFloat(args[8].substring(args[8].indexOf('=') + 1));
		int MAX_ITERATIONS = Integer.parseInt(args[9].substring(args[9].indexOf('=') + 1));
		
		Configuration conf = getConf();
		
		// convert edge file to adjList file.
		//EdgeToAdjListConverter job1 = new EdgeToAdjListConverter();
		String[] args1 = {edgeListPath,adjListPath,NUM_REDUCERS};
		//int rv = job1.run(args1);
		int rv = ToolRunner.run(conf,new EdgeToAdjListConverter(),args1);
		if(rv < 0){
			System.out.println("Error in Job EdgeToAdjListConverter");
			System.exit(rv);
		}
		
		if(isWeighted.charAt(0) == 'N' || isWeighted.charAt(0) == 'n'){			// unweighted graph
			// graph coloring
			//GRAPHCOLORING job2 = new GRAPHCOLORING();
			String[] args2 = {adjListPath,nodeColorsPath,NUM_NODES,NUM_REDUCERS,isWeighted};
			//rv = job2.run(args2);
			rv = ToolRunner.run(conf,new org.clustering.unweighted.gc.GRAPHCOLORING(), args2);		//rv holds number of iterations.
			if(rv < 0){
				System.out.println("Error in Job GRAPHCOLORING");
				System.exit(rv);
			}
			
			//graph matching
			String[] args3 = {adjListPath,nodeColorsPath,matchingPath,NUM_REDUCERS,Integer.toString(rv)};
			rv = ToolRunner.run(conf,new org.clustering.unweighted.matching.Matching1(), args3);
			if(rv < 0){
				System.out.println("Error in Job GRAPHCOLORING");
				System.exit(rv);
			}
		}
		else{
		// graph coloring
			String[] args2 = {adjListPath,nodeColorsPath,NUM_NODES,NUM_REDUCERS};
			rv = ToolRunner.run(conf,new org.clustering.weighted.gc.GRAPHCOLORING(), args2);
			if(rv < 0){
				System.out.println("Error in Job GRAPHCOLORING");
				System.exit(rv);
			}
		}
		
		return 0;
	}
}