package pageRank;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.*;

import java.util.Scanner;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Counters;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.math.BigDecimal;
import java.net.URI; 
import java.io.*;

public class DanglingMapper extends Mapper<Text, Text, Text, DoubleWritable> {
	
	private Text K = new Text("104021229");
	private DoubleWritable V = new DoubleWritable();
	private double res = 0;
	private ArrayList<Double> vec = new ArrayList<Double>();
	private int N;
	
	public static double add(double v1, double v2) {
		BigDecimal b1 = new BigDecimal(Double.toString(v1));
		BigDecimal b2 = new BigDecimal(Double.toString(v2));
		return b1.add(b2).doubleValue();
	}
	
	protected void setup(Context context) throws IOException, InterruptedException {
        N = context.getConfiguration().getInt("N", PageRank.N);
    }
	
	public void map(Text key, Text value, Context context) throws IOException, InterruptedException{		
		String[] arr = value.toString().split("#"); // PR, outDegree, out1, ...
		int outDegree = Integer.parseInt(arr[1]);
		if(outDegree==0){
			vec.add((Double.parseDouble(arr[0])/N));
		}
	}
	
	protected void cleanup(Context context) throws IOException, InterruptedException {
		Collections.sort(vec);
		for(int i=0 ; i<vec.size() ; ++i){
			res = add(res, vec.get(i));
		}
		V.set(res);
		context.write(K, V);
	}
}