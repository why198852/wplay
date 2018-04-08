package com.wplay.hadoop.mapreduce;

import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * 
 * @author James
 *
 * @param <KEY>
 * @param <VALUE>
 */
public class BasicReducer<KEY, VALUE> extends Reducer<KEY, VALUE, KEY, VALUE>{

	@Override
	protected void reduce(KEY key, Iterable<VALUE> values,Context context)
			throws IOException, InterruptedException {
		for(VALUE value : values){
			context.write(key, value);
		}
	}

	
}