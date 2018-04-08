package com.wplay.hbase.mapper;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class HbaseMapper<KEYOUT, VALUEOUT> extends Mapper<ImmutableBytesWritable,Result,KEYOUT, VALUEOUT>{

	@Override
	protected void map(ImmutableBytesWritable key, Result value,Context context)
			throws IOException, InterruptedException {
		
	}
}
