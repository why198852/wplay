package com.wplay.hadoop.mapreduce.unsplit;

import com.wplay.hadoop.mapreduce.input.FileKeyLineRecordReader;
import com.wplay.hadoop.vo.FileKey;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

/**
 * ²»ÇÐ¸î InputFormat
 * @author James
 *
 */
public class UnSplitInputFormat extends FileInputFormat<FileKey, Text>{

	public RecordReader<FileKey, Text> createRecordReader(InputSplit split,TaskAttemptContext context) {
		return new FileKeyLineRecordReader();
	}

	protected boolean isSplitable(JobContext context, Path file) {
		return false;
	}

}
