package pageRank;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueLineRecordReader;

import java.io.*;


public class BuildGraph{
	
	public BuildGraph(){

  	}
	
	public void BuildGraph(String[] args) throws Exception {
		
		Configuration conf = new Configuration();
		
		// Parse Input
		conf.set(TextOutputFormat.SEPERATOR, "#");
		Job job1 = Job.getInstance(conf, "ParseInput");
		job1.setJarByClass(BuildGraph.class);

		job1.setMapperClass(ParseMapper.class);
		job1.setReducerClass(ParseReducer.class);
		
		job1.setMapOutputKeyClass(Text.class);
		job1.setMapOutputValueClass(Text.class);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(Text.class);
		
		job1.setNumReduceTasks(32);
		
		FileInputFormat.addInputPath(job1, new Path(args[0]));
		FileOutputFormat.setOutputPath(job1, new Path("pair"));
		
		job1.waitForCompletion(true);
		
		FileUtil.copyMerge(FileSystem.get(conf), new Path("pair"), FileSystem.get(conf), new Path("adjPair"), true, conf, null);
		
		PageRank.N = (int)job1.getCounters().findCounter(testM.exist).getValue();
		System.out.println(PageRank.N);
		
		// Build Adjcency List
		conf.setInt("N", PageRank.N);
		conf.setStrings(KeyValueLineRecordReader.KEY_VALUE_SEPERATOR, "#");
		Job job2 = Job.getInstance(conf, "Init");
		job2.setJarByClass(BuildGraph.class);
		
		job2.setInputFormatClass(KeyValueTextInputFormat.class);	

		job2.setMapperClass(InitMapper.class);
		job2.setReducerClass(InitReducer.class);
		
		job2.setMapOutputKeyClass(Text.class);
		job2.setMapOutputValueClass(Text.class);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(AdjList.class);
		
		job2.setNumReduceTasks(32);
		
		FileInputFormat.addInputPath(job2, new Path("adjPair"));
		FileOutputFormat.setOutputPath(job2, new Path("init"));
		
		job2.waitForCompletion(true);
		
		FileUtil.copyMerge(FileSystem.get(conf), new Path("init"), FileSystem.get(conf), new Path("adjList0"), true, conf, null);
		
		Path p = new Path("adjPair");
		FileSystem fs = p.getFileSystem(conf);
		fs.delete(p, true);
		
		PageRank.D = (int)job2.getCounters().findCounter(testR.dangling).getValue();
		System.out.println(PageRank.D);	
	}
}