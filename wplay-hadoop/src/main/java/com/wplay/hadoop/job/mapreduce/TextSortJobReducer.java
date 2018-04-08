package com.wplay.hadoop.job.mapreduce;

import com.wplay.hadoop.vo.DescLongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * 
 * @author James
 *
 */
public class TextSortJobReducer extends Reducer<DescLongWritable, Text, Text, DescLongWritable>{

	@Override
	protected void reduce(DescLongWritable key, Iterable<Text> values,Context context)
			throws IOException, InterruptedException {
		for(Text value : values){
			context.write(value , key);
		}
	}
}
