/**
 * Copyright    : Copyright. @ 2008-2013 Skycloud Technology (China). Inc.All rights Reserved
 * URL          : http://www.chinaskycloud.com/
 * Create time  : 2013-5-14
 */
package com.wplay.hadoop.mapreduce.input;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.SplittableCompressionCodec;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.util.LineReader;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author James
 * @date 2013-05-13
 * @param <V>
 * @param <K>
 * 
 */
public class LimitMapperInputFormat<K, V> extends FileInputFormat<K, V> {
	private static final Log LOG = LogFactory
			.getLog(LimitMapperInputFormat.class);
	private static final double SPLIT_SLOP = 1.1; // 10% slop
	static final String NUM_INPUT_FILES = "mapreduce.input.num.files";

	@Override
	protected boolean isSplitable(JobContext context, Path file) {
		final CompressionCodec codec = new CompressionCodecFactory(
				context.getConfiguration()).getCodec(file);
		if (null == codec) {
			return true;
		}
		return codec instanceof SplittableCompressionCodec;
	}

	@Override
	public RecordReader<K, V> createRecordReader(InputSplit split,
			TaskAttemptContext context) throws IOException,
			InterruptedException {
		
		if (split instanceof FileSplit) {
			String delimiter = context.getConfiguration().get(
					"textinputformat.record.delimiter");
			byte[] recordDelimiterBytes = null;
			if (null != delimiter)
				recordDelimiterBytes = delimiter.getBytes();
			return (RecordReader<K, V>) new LineRecordReader(
					recordDelimiterBytes);
		} else if (split instanceof CombineFileSplit) {
			return new CombineFileRecordReader((CombineFileSplit) split,
					context, CombineFileLineRecordReader.class);
		}
		return null;

	}

	/**
	 * Generate the list of files and make them into FileSplits.
	 */
	public List<InputSplit> getSplits(JobContext job) throws IOException {

		long minSize = Math.max(getFormatMinSplitSize(), getMinSplitSize(job));
		long maxSize = getMaxSplitSize(job);
		// generate splits
		List<InputSplit> splits = new ArrayList<InputSplit>();

		List<FileStatus> files = listStatus(job);

		if (files.size() == 0) {
			return splits;
		}
		int mappers = job.getConfiguration().getInt("mapred.map.tasks", 0);
		if (mappers == 0) {
			return super.getSplits(job);
		}

		boolean isAllSplit = true;
		boolean isAllCompress = true;
		for (FileStatus file : files) {
			Path path = file.getPath();
			if (isSplitable(job, path)) {
				isAllCompress = false;
			} else {
				isAllSplit = false;
			}

		}
		/**
		 * 判断是否是全部非压缩 或 全部压缩，如果同时有两种格式的文件，将不处理
		 */
		if (!isAllCompress && !isAllSplit) {
			LOG.info("There are two file format" + "");
			return super.getSplits(job);
		}
		/**
		 * 如果全是压缩文件，但是定义的map数大于文件数，涉及到切分，由于gz是不支持切分的，所以也不处理
		 */
		else if (isAllCompress && mappers >= files.size()) {
			LOG.info("");
			return super.getSplits(job);
		}

		/**
		 * 全是非压缩文件： files = mappers : passed
		 */
		if (isAllSplit && files.size() == mappers) {
			LOG.info("files = mappers ..........");
			for (FileStatus file : files) {

				Path path = file.getPath();
				FileSystem fs = path.getFileSystem(job.getConfiguration());
				long length = file.getLen();
				BlockLocation[] blkLocations = fs.getFileBlockLocations(file,
						0, length);

				// split per file
				splits.add(new FileSplit(path, 0, length, blkLocations[0]
						.getHosts()));
			}
		}
		/**
		 * 全是非压缩文件： files < mappers
		 */
		if (isAllSplit && files.size() < mappers) {
			LOG.info("files < mappers ..........");
			long totalLength = 0;

			for (FileStatus file : files) {
				totalLength += file.getLen();
			}
			if (totalLength == 0) {
				return null;
			}
			// long avgLength = totalLength / files.size();
			int count = 1;
			long splitSize = 0;
			for (FileStatus file : files) {

				Path path = file.getPath();
				FileSystem fs = path.getFileSystem(job.getConfiguration());
				long length = file.getLen();
				BlockLocation[] blkLocations = fs.getFileBlockLocations(file,
						0, length);
				/**
				 * This is the key operation, when maps defined by client
				 * greater than files size. The first maps % files.size
				 * files,split per file in (maps / files.size + 1) file. The
				 * left files split per file in (maps / files.size) file. That's
				 * all ,thx!
				 */
				if (count <= mappers % files.size()) {
					splitSize = length / (mappers / files.size() + 1);

				} else {
					splitSize = length / (mappers / files.size());
				}

				long bytesRemaining = length;
				while (((double) bytesRemaining) / splitSize > SPLIT_SLOP) {
					int blkIndex = getBlockIndex(blkLocations, length
							- bytesRemaining);
					splits.add(new FileSplit(path, length - bytesRemaining,
							splitSize, blkLocations[blkIndex].getHosts()));
					bytesRemaining -= splitSize;
				}

				if (bytesRemaining != 0) {
					splits.add(new FileSplit(path, length - bytesRemaining,
							bytesRemaining,
							blkLocations[blkLocations.length - 1].getHosts()));
				}

				count++;
			}

			// return splits;
		}
		/**
		 * 如果全是压缩文件 或者 全是非压缩文件，定义的map数小于文件数，进行合并 操作。 files > mappers :passed
		 */
		if ((isAllSplit || isAllCompress) && files.size() > mappers) {
			LOG.info("files > mappers ..........");
			int i = 0;
			int combineN = 1;
			Path[] fl = null;
			long[] offset = null;
			long[] lengths = null;
			String[] locations = null;

			for (FileStatus file : files) {

				Path path = file.getPath();
				FileSystem fs = path.getFileSystem(job.getConfiguration());
				long length = file.getLen();
				BlockLocation[] blkLocations = fs.getFileBlockLocations(file,
						0, length);

				if (files.size() % mappers == 0) {
					/**
					 * files%maps==0 ,combine per m/n files
					 */
					int m = files.size() / mappers;
					// combine per m/n files
					if (combineN == 1) {
						// init combine split params
						fl = new Path[files.size() / mappers];
						offset = new long[files.size() / mappers];
						lengths = new long[files.size() / mappers];
						locations = new String[files.size() / mappers];

					}
					fl[combineN - 1] = path;
					offset[combineN - 1] = 0;
					lengths[combineN - 1] = length;
					locations[combineN - 1] = blkLocations[0].getHosts()[0];//
					if (combineN == (files.size() / mappers)) {
						CombineFileSplit combinesplit = new CombineFileSplit(
								fl, offset, lengths, locations);
						splits.add(combinesplit);
						combineN = 0;
						fl = null;
						offset = null;
						lengths = null;
						locations = null;

					}

				} else {
					/**
					 * files%maps!=0,example as: m files n maps,so combine
					 * m%n*(m/n+1) + (n-m%n)*(m/n)
					 */

					if (i < files.size() % mappers
							* (files.size() / mappers + 1)) {
						// combine m/n+1 files
						if (combineN == 1) {
							// init combine split params
							fl = new Path[files.size() / mappers + 1];
							offset = new long[files.size() / mappers + 1];
							lengths = new long[files.size() / mappers + 1];
							locations = new String[files.size() / mappers + 1];

						}
						fl[combineN - 1] = path;
						offset[combineN - 1] = 0;
						lengths[combineN - 1] = length;
						locations[combineN - 1] = blkLocations[0].getHosts()[0];
						if (combineN == (files.size() / mappers + 1)) {
							CombineFileSplit combinesplit = new CombineFileSplit(
									fl, offset, lengths, locations);
							splits.add(combinesplit);
							combineN = 0;
							fl = null;
							offset = null;
							lengths = null;
							locations = null;
						}

					} else {
						// combine m/n files
						if (combineN == 1) {
							// init combine split params
							fl = new Path[files.size() / mappers];
							offset = new long[files.size() / mappers];
							lengths = new long[files.size() / mappers];
							locations = new String[files.size() / mappers];

						}
						fl[combineN - 1] = path;
						offset[combineN - 1] = 0;
						lengths[combineN - 1] = length;
						locations[combineN - 1] = blkLocations[0].getHosts()[0];
						if (combineN == (files.size() / mappers)) {
							CombineFileSplit combinesplit = new CombineFileSplit(
									fl, offset, lengths, locations);
							splits.add(combinesplit);
							combineN = 0;
							fl = null;
							offset = null;
							lengths = null;
							locations = null;
						}
					}
				}
				//
				i++;
				combineN++;
			}
		}

		// Save the number of input files in the job-conf
		job.getConfiguration().setLong(NUM_INPUT_FILES, files.size());
		// LOG.info(splits.size() + "---------------------------------------");
		LOG.info("Total # of splits: " + splits.size());
		return splits;
	}

	/**
	 * RecordReader is responsible from extracting records from a chunk of the
	 * CombineFileSplit.
	 */
	public static class CombineFileLineRecordReader extends
			RecordReader<LongWritable, Text> {
		private CompressionCodecFactory compressionCodecs = null;
		private long startOffset; // offset of the chunk;
		private long end; // end of the chunk;
		private long pos; // current pos
		private FileSystem fs;
		private Path path;
		private LongWritable key;
		private Text value;

		private FSDataInputStream fileIn;
		private LineReader reader;

		public CombineFileLineRecordReader(CombineFileSplit split,
				TaskAttemptContext context, Integer index) throws IOException {

			fs = FileSystem.get(context.getConfiguration());
			this.path = split.getPath(index);
			this.startOffset = split.getOffset(index);
			this.end = startOffset + split.getLength(index);
			boolean skipFirstLine = false;
			Configuration job = context.getConfiguration();
			compressionCodecs = new CompressionCodecFactory(job);
			final CompressionCodec codec = compressionCodecs.getCodec(path);
			
			String delimiter = context.getConfiguration().get(
					"textinputformat.record.delimiter");
			byte[] recordDelimiterBytes = null;
			if (null != delimiter)
				recordDelimiterBytes = delimiter.getBytes();
			// open the file
			fileIn = fs.open(path);

			if (startOffset != 0) {
				skipFirstLine = true;
				--startOffset;
				fileIn.seek(startOffset);
			}
			if (codec != null) {
				if (null == recordDelimiterBytes) {
					reader = new LineReader(codec.createInputStream(fileIn), job);
				} else {
					reader = new LineReader(codec.createInputStream(fileIn), job,
							recordDelimiterBytes);
				}
				end = Long.MAX_VALUE;
			} else {
				reader = new LineReader(fileIn);
			}
			if (skipFirstLine) { // skip first line and re-establish
									// "startOffset".
				startOffset += reader.readLine(
						new Text(),
						0,
						(int) Math.min((long) Integer.MAX_VALUE, end
								- startOffset));
			}
			this.pos = startOffset;
		}

		public void initialize(InputSplit split, TaskAttemptContext context)
				throws IOException, InterruptedException {
		}

		public void close() throws IOException {
		}

		public float getProgress() throws IOException {
			if (startOffset == end) {
				return 0.0f;
			} else {
				return Math.min(1.0f, (pos - startOffset)
						/ (float) (end - startOffset));
			}
		}

		public boolean nextKeyValue() throws IOException {
			if (key == null) {
				key = new LongWritable();
			}
			if (value == null) {
				value = new Text();
			}
			int newSize = 0;
			if (pos < end) {
				newSize = reader.readLine(value);
				pos += newSize;
				key.set(key.get() + newSize);
			}
			if (newSize == 0) {
				key = null;
				value = null;
				return false;
			} else {
				return true;
			}
		}

		public Text getCurrentValue() throws IOException, InterruptedException {
			return value;
		}

		/*
		 * (non-Javadoc)
		 * 
		 * @see org.apache.hadoop.mapreduce.RecordReader#getCurrentKey()
		 */
		@Override
		public LongWritable getCurrentKey() throws IOException,
				InterruptedException {
			// TODO Auto-generated method stub
			return key;
		}
	}
}
