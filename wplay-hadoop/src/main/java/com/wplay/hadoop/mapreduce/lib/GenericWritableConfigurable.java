package com.wplay.hadoop.mapreduce.lib;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public abstract class GenericWritableConfigurable extends Configured implements
		Writable, Configurable {
	private static final String NOT_SET = null;

	private String type = NOT_SET;
	private Writable instance;

	public void set(Writable obj) {
		this.instance = obj;
		this.type = this.instance.getClass().getName();
	}

	public Writable get() {
		return this.instance;
	}

	public String toString() {
		return new StringBuilder()
				.append("GW[")
				.append(this.instance != null ? new StringBuilder()
						.append("class=")
						.append(this.instance.getClass().getName())
						.append(",value=").append(this.instance.toString())
						.toString() : "(null)").append("]").toString();
	}

	public void write(DataOutput out) throws IOException {
		if ((this.type == NOT_SET) || (this.instance == null)) {
			throw new IOException(
					new StringBuilder()
							.append("The GenericWritable has NOT been set correctly. type=")
							.append(this.type).append(", instance=")
							.append(this.instance).toString());
		}

		Text.writeString(out, this.type);
		this.instance.write(out);
	}

	public void readFields(DataInput in) throws IOException {
		this.type = Text.readString(in);
		try {
			set((Writable) Class.forName(this.type).newInstance());
		} catch (Exception e) {
			e.printStackTrace();
			throw new IOException(new StringBuilder()
					.append("Cannot initialize the class: ").append(this.type)
					.toString());
		}
		Writable w = get();
		if ((w instanceof Configurable))
			((Configurable) w).setConf(getConf());
		w.readFields(in);
	}
}