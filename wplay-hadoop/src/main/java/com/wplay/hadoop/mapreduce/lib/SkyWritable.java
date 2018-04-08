package com.wplay.hadoop.mapreduce.lib;

import org.apache.hadoop.io.Writable;

public class SkyWritable extends GenericWritableConfigurable {
	public SkyWritable() {
	}

	public SkyWritable(Writable instance) {
		set(instance);
	}
}