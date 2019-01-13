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
import java.net.URI; 
import java.io.*;

public class SortMapper extends Mapper<Text, Text, SortPair, NullWritable> {
	
	private SortPair K = new SortPair();
	
	public void map(Text key, Text value, Context context) throws IOException, InterruptedException{		
		String[] arr = value.toString().split("#"); // PR, outDegree, out1, ...
		K.setKey(key.toString());
		K.setPR(Double.parseDouble(arr[0]));
		context.write(K, NullWritable.get());
	}
}