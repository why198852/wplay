package com.wplay.hadoop.vo;

import com.wplay.core.util.StringUtil;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 
 * @author James
 *
 */
public class FileKey implements WritableComparable<FileKey>{
	private String path;
	private long pos;
	private String outPath;
	
	public String getPath() {
		return path;
	}

	public void setPath(String path) {
		this.path = path;
	}
	
	public String getOutPath() {
		return outPath;
	}

	public void setOutPath(String outPath) {
		this.outPath = outPath;
	}

	public long getPos() {
		return pos;
	}

	public void setPos(long pos) {
		this.pos = pos;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		Text.writeString(out, StringUtil.toString(this.path));
		out.writeLong(this.pos);
		Text.writeString(out, StringUtil.toString(outPath));
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.path = Text.readString(in);
		this.pos = in.readLong();
		this.outPath = Text.readString(in);
	}

	@Override
	public int compareTo(FileKey other) {
		long n = this.pos - other.pos;
		if(n == 0){
			return StringUtil.compare(this.path, other.path);
		}else{
			return n > 0 ? 1 : -1;
		}
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((path == null) ? 0 : path.hashCode());
		result = prime * result + (int) (pos ^ (pos >>> 32));
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		FileKey other = (FileKey) obj;
		if (path == null) {
			if (other.path != null)
				return false;
		} else if (!path.equals(other.path))
			return false;
		if (pos != other.pos)
			return false;
		return true;
	}

	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append(path);
		builder.append("|+|");
		builder.append(pos);
		return builder.toString();
	}
}
