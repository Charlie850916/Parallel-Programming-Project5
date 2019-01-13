package pageRank;

import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

import java.util.*;
import java.math.BigDecimal;

public class ErrorReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
	
	private Text K = new Text();
	private DoubleWritable V = new DoubleWritable();
	private double error = 0;
	
	public static double add(double v1, double v2) {
		BigDecimal b1 = new BigDecimal(Double.toString(v1));
		BigDecimal b2 = new BigDecimal(Double.toString(v2));
		return b1.add(b2).doubleValue();
	}
	
    public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
		for(DoubleWritable d : values){
			error = add(error, d.get());
		}
		V.set(error);
		context.write(K,V);
	}
}
