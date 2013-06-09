package org.clustering.utilities;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.*; 
public class FloatArrayWritable implements Writable {
	private float[] data;
	
	public FloatArrayWritable(){
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
	  public String toString() {
		  StringBuffer sb = new StringBuffer();
		  for(int i = 0;i < data.length;i++){
			  sb.append(" ").append(data[i]);
		  }
		  sb.deleteCharAt(0);
		return sb.toString();
	  }
}
