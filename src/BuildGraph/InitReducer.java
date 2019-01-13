package pageRank;

import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

import java.util.*;

public class InitReducer extends Reducer<Text, Text, Text, AdjList> {
	
	private Text K = new Text();
	private AdjList V = new AdjList();
	private int N;
	
    protected void setup(Context context) throws IOException, InterruptedException {
        N = context.getConfiguration().getInt("N", PageRank.N);
    }
	
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		K.set(key.toString());
		V.clear();
		V.setPR(1.0/N);
		for(Text t : values){
			String s = t.toString();
			if(!s.equals("104021229")){
				V.addString(s);
			}
		}
		if(V.outDegree()==0){
			context.getCounter(testR.dangling).increment(1);
		}
		context.write(K, V);
	}
}

enum testR{
	dangling;
}