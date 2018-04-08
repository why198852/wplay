package com.wplay.hadoop.mapreduce.output;

import com.wplay.core.util.StringUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.DataOutputStream;
import java.io.IOException;

/**
 * °ÑValue ºÍ Key Êä³ö
 * @author James
 * 
 */
public class VKLineOutputFormat<K, V> extends FileOutputFormat<K, V> {

	public static final String NEW_LINE = "\n";
	public static final String SPLIT = "\t";
	public static final String OUTPUT_SPLIT = "com.sky.output.split";

	protected static class LineRecordWriter<K, V> extends RecordWriter<K, V> {
		private final DataOutputStream fileWrite;
		private final String split;

		LineRecordWriter(DataOutputStream fileWrite,String split) {
			this.fileWrite = fileWrite;
			this.split = split;
		}

		@Override
		public synchronized void write(K key, V value) throws IOException,
				InterruptedException {
			fileWrite.write(StringUtil.toBytes(value + split + key + NEW_LINE));
		}

		@Override
		public synchronized void close(TaskAttemptContext context)
				throws IOException, InterruptedException {
			fileWrite.close();
		}
	}

	@Override
	public RecordWriter<K, V> getRecordWriter(TaskAttemptContext context)
			throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();
		final String split = conf.get(OUTPUT_SPLIT, SPLIT);
		boolean isCompressed = getCompressOutput(context);
		CompressionCodec codec = null;
		String extension = "";
		if (isCompressed) {
			Class<? extends CompressionCodec> codecClass = getOutputCompressorClass(
					context, GzipCodec.class);
			codec = (CompressionCodec) ReflectionUtils.newInstance(codecClass,
					conf);
			extension = codec.getDefaultExtension();
		}
		Path path = getOutputPath(context);
		FileSystem fs = path.getFileSystem(conf);
		Path file = getDefaultWorkFile(context, extension);
		FSDataOutputStream fileWrite = fs.create(
				new Path(path, file.getName()), false);
		if (!isCompressed) {
			return new LineRecordWriter<K, V>(fileWrite,split);
		} else {
			return new LineRecordWriter<K, V>(new DataOutputStream(
					codec.createOutputStream(fileWrite)),split);
		}
	}
}
