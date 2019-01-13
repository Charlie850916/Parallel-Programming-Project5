package pageRank;

import java.io.IOException;
import java.util.StringTokenizer;

import java.util.Scanner;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;

import java.util.*;
import java.net.URI; 
import java.io.*;

public class PageRank{
	
	public static double alpha = 0.85;
	public static int N;
	public static int D;
	public static int iter;
	public static double danglingValue = 0;
	public static double error = 1;
	
    public static void main(String[] args) throws Exception {
		
		BuildGraph job1 = new BuildGraph();
        job1.BuildGraph(args);
		
		iter =  Integer.parseInt(args[1]);
		if(iter==-1){
			iter = 30;
		}
		
		for(int i=0 ; i<iter ; ++i){
			System.out.println("Iter = "+i);
			CalculatePR job2 = new CalculatePR();
			job2.CalculatePR("adjList"+i, i);
			if(error < 0.001){
				iter = i+1;
				break;
			}
		}
		
		SortPR job3 = new SortPR();
		job3.SortPR(args, "adjList"+iter);
		
		System.exit(0);
    }  
}