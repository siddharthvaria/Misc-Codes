package org.clustering.utilities;

import org.apache.hadoop.io.*;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class IntArrayWritable implements Writable {
	private int[] data;
	
	public IntArrayWritable(){
		data = null;
	}
	
	public int[] get(){
		return this.data;
	}
	public void set(int[] d){
		this.data = d;
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		//Deserialize the fields of this object from in.
		int len = in.readInt();
		this.data = new int[len];
		for(int i = 0;i < len;i++){
			data[i] = in.readInt();
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
			out.writeInt(data[i]);
		}
	}
	  public String toString() {
		  StringBuffer sb = new StringBuffer();
		  for(int i = 0;i < data.length;i++){
			  sb.append(" ").append(data[i]);
		  }
		  sb.deleteCharAt(0);
		return sb.toString();
	  }
}