package com.wplay.hbase.vo;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;

public class HbExportVO implements Writable, Iterable<String> {
	private static byte VERSION = 1;
	private String tableName;
	private Collection<String> families = new LinkedList();

	public String getTableName() {
		return this.tableName;
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

	public void addFamily(String family) {
		this.families.add(family);
	}

	public void readFields(DataInput in) throws IOException {
		byte v = in.readByte();
		if (v == VERSION) {
			this.tableName = Text.readString(in);
			int size = in.readInt();
			for (int i = 0; i < size; i++)
				this.families.add(Text.readString(in));
		}
	}

	public void write(DataOutput out) throws IOException {
		out.writeByte(VERSION);
		Text.writeString(out, this.tableName);
		out.writeInt(this.families.size());
		for (String family : this.families)
			Text.writeString(out, family);
	}

	public Iterator<String> iterator() {
		return this.families.iterator();
	}
}