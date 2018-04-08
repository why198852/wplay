package com.wplay.hadoop.vo;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class FileInfo implements WritableComparable<FileInfo> {
	private String path;
	
	public String getPath() {
		return this.path;
	}

	public void setPath(String path) {
		this.path = path;
	}

	public int hashCode() {
		int result = 1;
		result = 31 * result + (this.path == null ? 0 : this.path.hashCode());
		return result;
	}

	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		FileInfo other = (FileInfo) obj;
		if (this.path == null) {
			if (other.path != null)
				return false;
		} else if (!this.path.equals(other.path))
			return false;
		return true;
	}

	public void readFields(DataInput in) throws IOException {
		this.path = WritableUtils.readString(in);
	}

	public void write(DataOutput out) throws IOException {
		WritableUtils.writeString(out, path);
	}

	public int compareTo(FileInfo other) {
		if (this.path == null)
			return -1;
		if ((other == null) || (other.path == null)) {
			return 1;
		}
		return this.path.compareTo(other.path);
	}
}