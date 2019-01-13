package pageRank;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import java.util.*;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.Text;

public class AdjList implements Writable{
	
	private double PR;
	private double error;
	private ArrayList<String> list;

	public AdjList() {
		PR = 0;
		error = 0;
		list = new ArrayList<String>();
	}
	
	public void addString(String s){
		list.add(s);
	}
	
	public void clear(){
		list.clear();
	}
	
	public int outDegree(){
		return list.size();
	}
	
	public void setPR(double p){
		PR = p;
	}
	
	public void setError(double e){
		error = e;
		if(error < 0){
			error = -error;
		}
	}
	
	@Override 
	public String toString(){
		String s = "";
		s += PR;
		s += "#";
		s += list.size();
		for(int i=0 ; i<list.size() ; ++i){
			s += "#";
			s += list.get(i);
		}
		s += "#";
		s += error;
		return s;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeDouble(PR);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		PR = in.readDouble();
	}
} 