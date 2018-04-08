package com.wplay.hadoop.mapreduce.output;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapred.FileAlreadyExistsException;
import org.apache.hadoop.mapred.InvalidJobConfException;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.IOException;

/**
 * 
 * @author James
 *
 */
public class MapFileOutputFormat extends FileOutputFormat<WritableComparable<?>, Writable> {

	@Override
	public void checkOutputSpecs(JobContext job)
			throws FileAlreadyExistsException, IOException {
		Path outDir = getOutputPath(job);
		if (outDir == null) {
			throw new InvalidJobConfException("Output directory not set.");
		}
		FileSystem fs = outDir.getFileSystem(job.getConfiguration());
		if (fs.exists(outDir)) {
			throw new FileAlreadyExistsException("Output directory " + outDir + " already exists");
		}
	}

	protected static class MapRecordWriter extends
			RecordWriter<WritableComparable<?>, Writable> {
		private final MapFile.Writer writer;

		MapRecordWriter(MapFile.Writer writer) {
			this.writer = writer;
		}

		@Override
		public synchronized void close(TaskAttemptContext context)
				throws IOException, InterruptedException {
			writer.close();
		}

		@Override
		public void write(WritableComparable<?> key, Writable value)
				throws IOException, InterruptedException {
			writer.append(key, value);
		}
	}

	public static CompressionType getOutputCompressionType(JobContext job) {
		String val = job.getConfiguration().get("mapred.output.compression.type",CompressionType.RECORD.toString());
		return CompressionType.valueOf(val);
	}

	@Override
	public RecordWriter<WritableComparable<?>, Writable> getRecordWriter(TaskAttemptContext job) throws IOException, InterruptedException {
		Configuration conf = job.getConfiguration();
		boolean isCompressed = getCompressOutput(job);
		CompressionCodec codec = null;
		String extension = "";
		CompressionType compressionType = CompressionType.NONE;
		if (isCompressed) {
			compressionType = getOutputCompressionType(job);
			Class<? extends CompressionCodec> codecClass = getOutputCompressorClass(job, GzipCodec.class);
			codec = (CompressionCodec) ReflectionUtils.newInstance(codecClass,conf);
			extension = codec.getDefaultExtension();
		}
		Path path = getOutputPath(job);
		FileSystem fs = path.getFileSystem(conf);
		Path file = getDefaultWorkFile(job, extension);
		Class<? extends WritableComparable> keyClass = job.getOutputKeyClass().asSubclass(WritableComparable.class);
		Class<?> valueClass = job.getOutputValueClass().asSubclass(Writable.class);
		MapFile.Writer writer = new MapFile.Writer(conf, fs, file.toString(),WritableComparator.get(keyClass), valueClass, compressionType,
				codec, job);
		return new MapRecordWriter(writer);
	}
}
