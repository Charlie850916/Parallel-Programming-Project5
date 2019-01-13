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

public class SortPR{
	
	public SortPR(){

  	}
	
	public void SortPR(String[] args, String src) throws Exception {
		
		Configuration conf = new Configuration();
		
		// Extract Information
		conf.setStrings(KeyValueLineRecordReader.KEY_VALUE_SEPERATOR, "#");
		conf.set(TextOutputFormat.SEPERATOR, "\t");
		conf.setInt("N", PageRank.N);
		Job job = Job.getInstance(conf, "Sort");
		job.setJarByClass(SortPR.class);

		job.setInputFormatClass(KeyValueTextInputFormat.class);	

		job.setMapperClass(SortMapper.class);
		job.setPartitionerClass(SortPartitioner.class);
		job.setReducerClass(SortReducer.class);
		
		job.setMapOutputKeyClass(SortPair.class);
		job.setMapOutputValueClass(NullWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);
		
		job.setNumReduceTasks(32);
		
		FileInputFormat.addInputPath(job, new Path(src));
		FileOutputFormat.setOutputPath(job, new Path("SortTmp"));
		
		job.waitForCompletion(true);
		
		FileUtil.copyMerge(FileSystem.get(conf), new Path("SortTmp"), FileSystem.get(conf), new Path("ans.out"), true, conf, null);
		
		Path p = new Path(src);
		FileSystem fs = p.getFileSystem(conf);
		fs.delete(p, true);
	}
}