package pageRank;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.*;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;

import org.apache.hadoop.mapreduce.Mapper;

public class SortPartitioner extends Partitioner<SortPair, NullWritable> {

	private double maxValue = 6e-6;

	@Override
	public int getPartition(SortPair key, NullWritable value, int numReduceTasks) {
		return 0;
	}
}
