package com.wplay.hadoop.job.join.vo;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Value implements Writable {
	private String value;
	private long index;

	public String getValue() {
		return value;
	}

	public void setValue(String value) {
		this.value = value;
	}

	public long getIndex() {
		return index;
	}

	public void setIndex(long index) {
		this.index = index;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.value = WritableUtils.readString(in);
		this.index = WritableUtils.readVLong(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		WritableUtils.writeString(out, value);
		WritableUtils.writeVLong(out, index);
	}

	@Override
	public String toString() {
		return value;
	}
}