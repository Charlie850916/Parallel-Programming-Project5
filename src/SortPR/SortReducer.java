package pageRank;

import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

import java.util.*;

public class SortReducer extends Reducer<SortPair, NullWritable, Text, DoubleWritable> {
	
	private Text K = new Text();
	private DoubleWritable V = new DoubleWritable();
	
    public void reduce(SortPair key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
		K.set(key.getKey());
		V.set(key.getPR());
		context.write(K,V);
	}
}
