package pageRank;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.FSDataInputStream;
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

public class CalculatePR{
	
	public CalculatePR(){

  	}
	
	public void CalculatePR(String src, int iter) throws Exception {
		
		Configuration conf = new Configuration();
		
		// Dangling Node
		conf.setStrings(KeyValueLineRecordReader.KEY_VALUE_SEPERATOR, "#");
		conf.setInt("N", PageRank.N);
		Job job1 = Job.getInstance(conf, "dangling");
		job1.setJarByClass(CalculatePR.class);

		job1.setInputFormatClass(KeyValueTextInputFormat.class);	

		job1.setMapperClass(DanglingMapper.class);
		job1.setReducerClass(DanglingReducer.class);
		
		job1.setMapOutputKeyClass(Text.class);
		job1.setMapOutputValueClass(DoubleWritable.class);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(DoubleWritable.class);
		
		FileInputFormat.addInputPath(job1, new Path(src));
		FileOutputFormat.setOutputPath(job1, new Path("danglingTmp"));
		
		job1.waitForCompletion(true);
		
		FileUtil.copyMerge(FileSystem.get(conf), new Path("danglingTmp"), FileSystem.get(conf), new Path("dangling"), true, conf, null);
		
		FileSystem fs = FileSystem.get(conf);
		Path inFile = new Path("dangling");
		FSDataInputStream in = fs.open(inFile);
		PageRank.danglingValue = Double.parseDouble(in.readLine());
		fs.delete(inFile, true);
		
		System.out.println("dangling = "+PageRank.danglingValue);
		
		// Build Adjcency List
		conf.setDouble("dangling", PageRank.danglingValue);
		conf.setDouble("alpha", PageRank.alpha);
		conf.set(TextOutputFormat.SEPERATOR, "#");
		Job job2 = Job.getInstance(conf, "PR");
		job2.setJarByClass(CalculatePR.class);
		
		job2.setInputFormatClass(KeyValueTextInputFormat.class);	

		job2.setMapperClass(PRMapper.class);
		job2.setCombinerClass(PRCombiner.class);
		job2.setReducerClass(PRReducer.class);
		
		job2.setMapOutputKeyClass(Text.class);
		job2.setMapOutputValueClass(InfoText.class);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(AdjList.class);
		
		job2.setNumReduceTasks(32);
		
		FileInputFormat.addInputPath(job2, new Path(src));
		FileOutputFormat.setOutputPath(job2, new Path("PRtmp"));
		
		job2.waitForCompletion(true);
		
		FileUtil.copyMerge(FileSystem.get(conf), new Path("PRtmp"), FileSystem.get(conf), new Path("adjList"+(iter+1)), true, conf, null);
		
		Path p = new Path(src);
		fs = p.getFileSystem(conf);
		fs.delete(p, true);
		conf.set(TextOutputFormat.SEPERATOR, "\t");
		Job job3 = Job.getInstance(conf, "error");
		job3.setJarByClass(CalculatePR.class);
		
		job3.setInputFormatClass(KeyValueTextInputFormat.class);	
		
		job3.setMapperClass(ErrorMapper.class);
		job3.setReducerClass(ErrorReducer.class);
		
		job3.setMapOutputKeyClass(Text.class);
		job3.setMapOutputValueClass(DoubleWritable.class);
		job3.setOutputKeyClass(Text.class);
		job3.setOutputValueClass(DoubleWritable.class);
		
		FileInputFormat.addInputPath(job3, new Path("adjList"+(iter+1)));
		FileOutputFormat.setOutputPath(job3, new Path("errorTmp"));
		
		job3.waitForCompletion(true);
		
		FileUtil.copyMerge(FileSystem.get(conf), new Path("errorTmp"), FileSystem.get(conf), new Path("error"), true, conf, null);
		
		fs = FileSystem.get(conf);
		inFile = new Path("error");
		in = fs.open(inFile);
		PageRank.error = Double.parseDouble(in.readLine());
		fs.delete(inFile, true);
		
		System.out.println("error = "+PageRank.error);
	}
}