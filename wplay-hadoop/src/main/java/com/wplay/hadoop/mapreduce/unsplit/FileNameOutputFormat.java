package com.wplay.hadoop.mapreduce.unsplit;

import com.wplay.hadoop.vo.FileKey;
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
import java.util.HashMap;
import java.util.Map;

public class FileNameOutputFormat<K extends FileKey, V> extends FileOutputFormat<K, V> {
	public static final String NEW_LINE = "\n";

	protected static class FileNameRecordWriter<K extends FileKey, V> extends RecordWriter<K, V> {
		private final Map<String,DataOutputStream> writers = new HashMap<String, DataOutputStream>();
		private final Path dir;
		private final String extension;
		private final boolean compressed;
		private final CompressionCodec codec;
		private final FileSystem fs;
		
		FileNameRecordWriter(Path dir,String extension,boolean compressed,CompressionCodec codec,Configuration conf) throws IOException{
			this.dir = dir;
			this.extension = extension;
			this.compressed = compressed;
			this.codec = codec;
			this.fs = dir.getFileSystem(conf);
		}

		@Override
		public synchronized void write(K key, V value) throws IOException,
				InterruptedException {
			String path = key.getPath();
			DataOutputStream writer = writers.get(path);
			if(writer == null){
				Path src = new Path(path);
				String name = src.getName();
				int index = name.lastIndexOf(".");
				String realName = name;
				if(index != -1){
					realName = name.substring(0, index);	
				}
				Path file = new Path(dir,realName + extension);
				FSDataOutputStream fileWrite = fs.create(file, false);
				if(compressed){
					writer = new DataOutputStream(codec.createOutputStream(fileWrite));
				}else{
					writer = fileWrite;
				}
				writers.put(path, writer);
			}
			writer.write(StringUtil.toBytes(value.toString() + NEW_LINE));
		}

		@Override
		public synchronized void close(TaskAttemptContext context)
				throws IOException, InterruptedException {
			for(DataOutputStream writer : writers.values()){
				writer.close();
			}
		}
	}

	@Override
	public RecordWriter<K, V> getRecordWriter(TaskAttemptContext context)
			throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();
		boolean isCompressed = getCompressOutput(context);
		CompressionCodec codec = null;
		String extension = "";
		if (isCompressed) {
			Class<? extends CompressionCodec> codecClass = getOutputCompressorClass(context, GzipCodec.class);
			codec = (CompressionCodec) ReflectionUtils.newInstance(codecClass,conf);
			extension = codec.getDefaultExtension();
		}
		Path path = getOutputPath(context);
		return new FileNameRecordWriter<K, V>(path,extension,isCompressed,codec,conf);
	}
	
	public static void main(String args[]){
		String name = "name.txt";
		System.out.println(name.substring(0, name.lastIndexOf(".")));
	}
}
