package com.wplay.hadoop.vo;

import org.apache.hadoop.io.LongWritable;

public class DescLongWritable extends LongWritable {
	public DescLongWritable() {
	}

	public DescLongWritable(LongWritable value) {
		set(value.get());
	}

	public DescLongWritable(long value) {
		set(value);
	}

	@Override
	public int compareTo(LongWritable o) {
		long thisValue = get();
		long thatValue = ((LongWritable) o).get();
		return thisValue == thatValue ? 0 : thatValue < thisValue ? -1 : 1;
	}
}