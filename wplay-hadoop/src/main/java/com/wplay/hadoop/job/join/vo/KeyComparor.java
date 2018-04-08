package com.wplay.hadoop.job.join.vo;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class KeyComparor extends WritableComparator {

	protected KeyComparor() {
		super(Key.class, true);
	}

	@Override
	public int compare(WritableComparable key1, WritableComparable key2) {
		byte[] aZhangh = Bytes.toBytes(((Key) key1).getZhangh());
		byte[] bZhangh = Bytes.toBytes(((Key) key2).getZhangh());
		return Bytes.compareTo(aZhangh, bZhangh);
	}
}