package com.wplay.hadoop.job.join.vo;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Key implements WritableComparable<Key> {

	private String zhangh;
	private long index;

	public String getZhangh() {
		return zhangh;
	}

	public long getIndex() {
		return index;
	}

	public void setIndex(long index) {
		this.index = index;
	}

	public void setZhangh(String zhangh) {
		this.zhangh = zhangh;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		zhangh = WritableUtils.readString(in);
		index = WritableUtils.readVLong(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		WritableUtils.writeString(out, zhangh);
		WritableUtils.writeVLong(out, index);
	}

	@Override
	public int hashCode() {
		return zhangh.hashCode();
	}

	@Override
	public int compareTo(Key o) {
		int re = this.zhangh.compareTo(o.zhangh);
		if (re == 0) {
			long r = this.index - o.index;
			if(r == 0){
				return 0;
			}else{
				return r > 0 ? 1 : -1;
			}
		} else {
			return re;
		}
	}
}