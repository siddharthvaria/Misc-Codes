package org.clustering.utilities;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.*;

public class IntArrayListWritable implements Writable{ 
	private ArrayList<Integer> data;
	
	public IntArrayListWritable(){
		data = null;
	}
	
	public ArrayList<Integer> get(){
		return this.data;
	}
	public void set(ArrayList<Integer> d){
		this.data = d;
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		//Deserialize the fields of this object from in.
		int len = in.readInt();
		this.data = new ArrayList<Integer>(len);
//		this.data = new int[len];
		for(int i = 0;i < len;i++){
//			data[i] = in.readInt();
			data.add(in.readInt());
		}
	}

	@Override
	public void write(DataOutput out) throws IOException {
		//Serialize the fields of this object to out.
		int len = 0;
		if(data != null){
			len = data.size();
		}
		out.writeInt(len);
		
		for(int i = 0;i < len;i++){
			out.writeInt(data.get(i));
		}
	}
	  public String toString() {
		  StringBuffer sb = new StringBuffer();
		  for(int i : data){
			  sb.append(" ").append(i);
		  }
		  sb.deleteCharAt(0);
		return sb.toString();
	  }
}
