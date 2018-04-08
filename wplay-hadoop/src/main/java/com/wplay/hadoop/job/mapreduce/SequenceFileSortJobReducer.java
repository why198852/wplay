package com.wplay.hadoop.job.mapreduce;

import com.wplay.hadoop.vo.DescLongWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * 
 * @author James
 *
 */
public class SequenceFileSortJobReducer extends Reducer<DescLongWritable, Text, Text, LongWritable> {
	
	protected void reduce(DescLongWritable key, Iterable<Text> iterable,Context context)
			throws IOException, InterruptedException {
		for(Text value :  iterable){
			context.write(value,key);
		}
	}
}