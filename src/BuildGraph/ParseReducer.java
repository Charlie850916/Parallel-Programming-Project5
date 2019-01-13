package pageRank;

import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

import java.util.*;

public class ParseReducer extends Reducer<Text, Text, Text, Text> {
	
	private Text K = new Text();
	private Text V = new Text();
	
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		ArrayList<String> arr = new ArrayList<String>(); 
		boolean valid = false;
		for(Text t : values){
			if(t.toString().equals("104021229")){
				valid = true;
			}
			arr.add(t.toString());
		}
		if(valid){
			V.set(key.toString());
			for(String s : arr){
				K.set(s);
				if(s.equals("104021229")){
					context.write(V,K);
				}else{
					context.write(K,V);
				}
			}
		}
	}
}