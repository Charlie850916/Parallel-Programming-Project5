package pageRank;

import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

import java.util.*;
import java.util.Collections;
import java.lang.*;
import java.math.BigDecimal;

public class PRReducer extends Reducer<Text, InfoText, Text, AdjList> {
	
	private Text K = new Text();
	private AdjList V = new AdjList();
	
	public static double add(double v1, double v2) {
		BigDecimal b1 = new BigDecimal(Double.toString(v1));
		BigDecimal b2 = new BigDecimal(Double.toString(v2));
		return b1.add(b2).doubleValue();
	}
	
    public void reduce(Text key, Iterable<InfoText> values, Context context) throws IOException, InterruptedException {
		K.set(key.toString());
		V.clear();
		
		double sum = 0, origin = 0;
		
		for(InfoText t : values){
			if(t.getType()==0){
				V.addString(t.getInfo());
			}else if(t.getType()==1){
				sum = add(sum, t.getValue());
			}else{
				origin = t.getValue();
			}
		}
		V.setPR(sum);
		V.setError(sum-origin);
		context.write(K, V);
	}
}
