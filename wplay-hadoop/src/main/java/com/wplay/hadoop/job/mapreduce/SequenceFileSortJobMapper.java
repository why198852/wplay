package com.wplay.hadoop.job.mapreduce;

import com.wplay.hadoop.vo.DescLongWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * 
 * @author James
 *
 */
public class SequenceFileSortJobMapper extends Mapper<Text, LongWritable, DescLongWritable, Text> {
	
	private DescLongWritable lastKey = new DescLongWritable(0l);
	
	protected void map(Text key, LongWritable value,Context context)
			throws IOException, InterruptedException {
		lastKey.set(value.get());
		context.write(lastKey, key);
	}
}