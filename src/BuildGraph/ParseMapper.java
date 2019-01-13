package pageRank;

import java.io.IOException;
import java.util.StringTokenizer;
import java.util.Scanner;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.Configuration;

import java.util.*;
import java.net.URI; 
import java.io.*;

public class ParseMapper extends Mapper<LongWritable, Text, Text, Text> {
	
	private Text K = new Text();
	private Text V = new Text();
	
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {		
		Pattern titlePattern = Pattern.compile("<title>(.+?)</title>");
		Matcher titleMatcher = titlePattern.matcher(unescapeXML(value.toString()));
		if(titleMatcher.find()){
			context.getCounter(testM.exist).increment(1);
			K.set(titleMatcher.group(1));
			Pattern linkPattern = Pattern.compile("\\[\\[(.+?)([\\|#]|\\]\\])");
			Matcher linkMatcher = linkPattern.matcher(unescapeXML(value.toString()));
			while(linkMatcher.find()){
				V.set(capitalizeFirstLetter(linkMatcher.group(1)));
				context.write(V, K);
			}
			V.set("104021229");
			context.write(K, V);
		}
	}
	
	private String unescapeXML(String input) {
		return input.replaceAll("&lt;", "<").replaceAll("&gt;", ">").replaceAll("&amp;", "&").replaceAll("&quot;", "\"").replaceAll("&apos;", "\'");
    }

    private String capitalizeFirstLetter(String input){

    	char firstChar = input.charAt(0);

        if ( firstChar >= 'a' && firstChar <='z'){
            if ( input.length() == 1 ){
                return input.toUpperCase();
            }
            else
                return input.substring(0, 1).toUpperCase() + input.substring(1);
        }
        else 
        	return input;
    }
}

enum testM{
	 exist
}
