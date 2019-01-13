package pageRank;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import java.util.*;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.Text;

public class InfoText implements Writable{
	
	private int type; // 0 for adj, 1 for value, 2 for originPR
	private String info;
	private double value;

	public InfoText() {
		info = "104021229";
	}
	
	public void setType(int t){
		type = t;
	}
	
	public void setInfo(String s){
		info = s;
	}
	
	public void setValue(double v){
		value = v;
	}
	
	public int getType(){
		return type;
	}

	public String getInfo(){
		return info;
	}
	
	public double getValue(){
		return value;
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(type);
		Text t = new Text(info);
		t.write(out);
		out.writeDouble(value);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		type = in.readInt();
		Text t = new Text();
		t.readFields(in);
		info = t.toString();
		value = in.readDouble();
	}
} 