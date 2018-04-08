package com.wplay.hadoop.mapreduce;

import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * 
 * @author James
 *
 * @param <KEY>
 * @param <VALUE>
 */
public class BasicMapper<KEY, VALUE> extends Mapper<KEY, VALUE,KEY, VALUE>{

	@Override
	protected void map(KEY key, VALUE value,Context context)
			throws IOException, InterruptedException {
		context.write(key, value);
	}

}