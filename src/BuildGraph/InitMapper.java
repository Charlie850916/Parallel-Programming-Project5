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

public class InitMapper extends Mapper<Text, Text, Text, Text> {
	
	private Text K = new Text();
	private Text V = new Text();
	
	public void map(Text key, Text value, Context context) throws IOException, InterruptedException {		
		K.set(key.toString());
		V.set(value.toString());
		context.write(K,V);
	}
}