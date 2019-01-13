package pageRank;

import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

import java.util.*;
import java.util.Collections;
import java.lang.*;
import java.math.BigDecimal;

public class PRCombiner extends Reducer<Text, InfoText, Text, InfoText> {
	
	private Text K = new Text();
	private InfoText V = new InfoText();
	
	public static double add(double v1, double v2) {
		BigDecimal b1 = new BigDecimal(Double.toString(v1));
		BigDecimal b2 = new BigDecimal(Double.toString(v2));
		return b1.add(b2).doubleValue();
	}
	
    public void reduce(Text key, Iterable<InfoText> values, Context context) throws IOException, InterruptedException {
		K.set(key.toString());
		
		double sum = 0;
		
		for(InfoText t : values){
			if(t.getType()==0){
				V.setInfo(t.getInfo());
				V.setType(0);
				context.write(K,V);
			}else if(t.getType()==1){
				sum = add(sum, t.getValue());
			}else{
				V.setValue(t.getValue());
				V.setType(2);
				context.write(K,V);
			}
		}
		
		V.setValue(sum);
		V.setType(1);
		context.write(K, V);
	}
}