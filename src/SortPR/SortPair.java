package pageRank;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import java.util.*;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.Text;

public class SortPair implements WritableComparable{
	
	private String key;
	private double PR;

	public SortPair() {	
	}
	
	public SortPair(String s, double p){
		key = s;
		PR = p;
	}
	
	public void setKey(String s){
		key = s;
	}
	
	public void setPR(double p){
		PR = p;
	}

	public String getKey(){
		return key;
	}
	
	public double getPR(){
		return PR;
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		Text t = new Text(key);
		t.write(out);
		out.writeDouble(PR);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		Text t = new Text();
		t.readFields(in);
		key = t.toString();
		PR = in.readDouble();
	}
	
	@Override
	public int compareTo(Object o) {

		double thisPR = this.getPR();
		double thatPR = ((SortPair)o).getPR();

		String thisKey = this.getKey();
		String thatKey = ((SortPair)o).getKey();

		if(thisPR < thatPR){
			return 1;
		}
		else if(thisPR == thatPR){
			return thisKey.compareTo(thatKey);
		}
		else{
			return -1;
		}
	}
} 