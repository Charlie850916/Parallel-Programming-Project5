package pageRank;

import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

import java.util.*;
import java.math.BigDecimal;
import java.util.Collections;

public class DanglingReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
	
	private Text K = new Text();
	private DoubleWritable V = new DoubleWritable();
	private ArrayList<Double> vec = new ArrayList<Double>();
	
	public static double add(double v1, double v2) {
		BigDecimal b1 = new BigDecimal(Double.toString(v1));
		BigDecimal b2 = new BigDecimal(Double.toString(v2));
		return b1.add(b2).doubleValue();
	}
	
    public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
		double dangling = 0.0;
		for(DoubleWritable d : values){
			vec.add(d.get());
		}
		Collections.sort(vec);
		for(int i=0 ; i<vec.size() ; ++i){
			dangling = add(dangling, vec.get(i));
		}
		V.set(dangling);
		context.write(K,V);
	}
}
