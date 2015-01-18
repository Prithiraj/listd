package com.ben.dl.listd;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class DomainListReducer extends MapReduceBase 
	implements Reducer<Text, LongWritable, Text,LongWritable>{

	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

	@Override
	public void reduce(Text key, Iterator<LongWritable> values,
			OutputCollector<Text, LongWritable> output, Reporter rep)
			throws IOException {
		// TODO Auto-generated method stub
		
	}

}
