package com.wplay.hadoop.mapreduce.input;

import com.wplay.hadoop.vo.FileInfo;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.LineReader;

import java.io.IOException;

/**
 * 
 * @author James
 *
 */
public class TextFileLineRecordReader extends RecordReader<FileInfo, Text> {
	private static final Log LOG = LogFactory
			.getLog(TextFileLineRecordReader.class);

	private CompressionCodecFactory compressionCodecs = null;
	private long start;
	private long pos;
	private long end;
	private LineReader in;
	private int maxLineLength;
	private FileInfo key = null;
	private Text value = null;
	private String path;

	public void initialize(InputSplit genericSplit, TaskAttemptContext context)
			throws IOException {
		FileSplit split = (FileSplit) genericSplit;
		Configuration job = context.getConfiguration();
		this.maxLineLength = job.getInt("mapred.linerecordreader.maxlength",2147483647);

		this.start = split.getStart();
		this.end = (this.start + split.getLength());
		Path file = split.getPath();
		FileSystem fs = file.getFileSystem(job);
		if (this.key == null) {
			this.key = new FileInfo();
		}
		this.path = file.toString();
		this.key.setPath(path);
		this.compressionCodecs = new CompressionCodecFactory(job);
		CompressionCodec codec = this.compressionCodecs.getCodec(file);

		FSDataInputStream fileIn = fs.open(split.getPath());
		boolean skipFirstLine = false;
		if (codec != null) {
			this.in = new LineReader(codec.createInputStream(fileIn), job);
			this.end = 9223372036854775807L;
		} else {
			if (this.start != 0L) {
				skipFirstLine = true;
				this.start -= 1L;
				fileIn.seek(this.start);
			}
			this.in = new LineReader(fileIn, job);
		}
		if (skipFirstLine) {
			this.start += this.in.readLine(new Text(), 0,
					(int) Math.min(2147483647L, this.end - this.start));
		}

		this.pos = this.start;
	}

	public boolean nextKeyValue() throws IOException {
		if(this.key == null){
			this.key = new FileInfo();
		}
		this.key.setPath(path);
		if (this.value == null) {
			this.value = new Text();
		}
		int newSize = 0;
		while (this.pos < this.end) {
			newSize = this.in.readLine(this.value, this.maxLineLength, Math.max((int) Math.min(2147483647L, this.end - this.pos),this.maxLineLength));
			if (newSize == 0) {
				break;
			}
			this.pos += newSize;
			if (newSize < this.maxLineLength) {
				break;
			}
			LOG.info("Skipped line of size " + newSize + " at pos "
					+ (this.pos - newSize));
		}
		if (newSize == 0) {
			this.key = null;
			this.value = null;
			return false;
		}
		return true;
	}

	public FileInfo getCurrentKey() {
		return this.key;
	}

	public Text getCurrentValue() {
		return this.value;
	}

	public float getProgress() {
		if (this.start == this.end) {
			return 0.0F;
		}
		return Math.min(1.0F, (float) (this.pos - this.start)
				/ (float) (this.end - this.start));
	}

	public synchronized void close() throws IOException {
		if (this.in != null)
			this.in.close();
	}
}