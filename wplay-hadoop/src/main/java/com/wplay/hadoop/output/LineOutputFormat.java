package com.wplay.hadoop.output;

import com.wplay.core.util.StringUtil;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.DataOutputStream;
import java.io.IOException;

public class LineOutputFormat<K,V> extends FileOutputFormat<K, V>{

	public static final String NEW_LINE = "\n";

	protected static class LineRecordWriter<K, V> implements RecordWriter<K, V> {
		private final DataOutputStream fileWrite;

		LineRecordWriter(DataOutputStream fileWrite) {
			this.fileWrite = fileWrite;
		}

		@Override
		public void close(Reporter reporter) throws IOException {
			fileWrite.close();
		}

		@Override
		public void write(K key, V value) throws IOException {
			fileWrite.write(StringUtil.toBytes(value.toString() + NEW_LINE));
		}
	}
	
	@Override
	public RecordWriter<K, V> getRecordWriter(FileSystem ignored, JobConf job,
			String name, Progressable progress) throws IOException {
		boolean isCompressed = getCompressOutput(job);
		CompressionCodec codec = null;
		String extension = "";
		if (isCompressed) {
			Class<? extends CompressionCodec> codecClass = getOutputCompressorClass(
					job, GzipCodec.class);
			codec = (CompressionCodec) ReflectionUtils.newInstance(codecClass,
					job);
			extension = codec.getDefaultExtension();
		}
		Path path = getOutputPath(job);
		FileSystem fs = path.getFileSystem(job);
		FSDataOutputStream fileWrite = fs.create(new Path(path, name + extension), false);
		if (!isCompressed) {
			return new LineRecordWriter<K, V>(fileWrite);
		} else {
			return new LineRecordWriter<K, V>(new DataOutputStream(
					codec.createOutputStream(fileWrite)));
		}
	}

}
