package com.wplay.hadoop.job.mapreduce;

import com.wplay.hadoop.vo.DescLongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.regex.Pattern;

public class TextSortJobMapper extends Mapper<WritableComparable<?>, Text, DescLongWritable, Text>{
	private static final String SPLIT = "\t|\\s";
	private static Pattern pattern = Pattern.compile(SPLIT);
	private DescLongWritable lastKey = new DescLongWritable(0);
	private Text lastValue = new Text();
	
	@Override
	protected void map(WritableComparable<?> key, Text value,Context context)
			throws IOException, InterruptedException {
		String vs[] = pattern.split(value.toString());
		if(vs.length == 2){
			lastKey.set(Long.parseLong(vs[0]));
			lastValue.set(vs[1]);
			context.write(lastKey, lastValue);
		}
	}
	
	public static void main(String args[]){
		String vs[] = pattern.split("75499	http://10.0.0.172/cgi-bin/mail_list?hittype=0&sid=_MriDmNCMgUcbmU12gR1dFXi%2C5%2Czg8E7iDzB&s=unread&folderid=1&flag=new&page=0&pagesize=10&ftype=&t=mail_list");
		System.out.println(vs.length);
		System.out.println(vs[0]);
		System.out.println(vs[1]);
	}
}
