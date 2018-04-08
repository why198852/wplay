package com.wplay.core.util;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class DateUtil {
	private static final Pattern secondPatt = Pattern.compile("^(\\d{1,})秒钟");
	private static final Pattern minPatt = Pattern.compile("^(\\d{1,})分钟");
	private static final Pattern hourPatt = Pattern.compile("^(\\d{1,})小时");
	private static final Pattern hourMinPatt = Pattern.compile("^(\\d{1,})小时(\\d{1,})分");
	private static final Pattern datePatt = Pattern.compile("^\\d{4}年\\d{2}月\\d{2}日");
	private static final Pattern todayPatt = Pattern.compile("^今天 (\\d{2}):(\\d{2})");
	private static final Pattern todayPattQ = Pattern.compile("(\\d{1,})天前");
	private static final String format = "MM月dd日 HH:mm";
	private static final String dateFormat = "yyyyMMddHHmmss";
	private static final SimpleDateFormat sdf = new SimpleDateFormat(format,Locale.US);
	private static final SimpleDateFormat sdateFormat = new SimpleDateFormat(dateFormat,Locale.US);

	private static final String DATEFORMAT[] =  new String [] {
		  "yyyyMMddHHmmss",
		  "yyyyMMdd",
		  "yyyy年 MM月 dd日, HH : mm",
		  "yyyy年MM月dd日, HH:mm",
		  "MM-dd HH:mm",
		  "M月d日 HH:mm",
		  "MM月d日 HH:mm",
		  "M月dd日 HH:mm",
		  "MM月dd日 HH:mm",
		  "MM月dd日HH时mm分",
		  "yyyy-MM-dd HH:mm:sszzzzzz",
		  "yyyy-MM-dd HH:mm:ss Z",
		  "yyyy-MM-dd HH:mm:ss",
		  "EEE MMM dd HH:mm:ss Z yyyy",
		  "EEE MMM dd HH:mm:ss yyyy",
	      "EEE MMM dd HH:mm:ss yyyy zzz",
	      "EEE, MMM dd HH:mm:ss yyyy zzz",
	      "EEE, dd MMM yyyy HH:mm:ss zzz",
	      "EEE,dd MMM yyyy HH:mm:ss zzz",
	      "EEE, dd MMM yyyy HH:mm:sszzz",
	      "EEE, dd MMM yyyy HH:mm:ss",
	      "EEE, dd-MMM-yy HH:mm:ss zzz",
	      "yyyy/MM/dd HH:mm:ss.SSS zzz",
	      "yyyy/MM/dd HH:mm:ss.SSS",
	      "yyyy/MM/dd HH:mm:ss zzz",
	      "yyyy/MM/dd",
	      "yyyy.MM.dd HH:mm:ss",
	      "yyyy-MM-dd HH:mm",
	      "MMM dd yyyy HH:mm:ss. zzz",
	      "MMM dd yyyy HH:mm:ss zzz",
	      "dd.MM.yyyy HH:mm:ss zzz",
	      "dd MM yyyy HH:mm:ss zzz",
	      "dd.MM.yyyy; HH:mm:ss",
	      "dd.MM.yyyy HH:mm:ss",
	      "dd.MM.yyyy zzz",
		  "yyyy-MM-dd",
		  "HH:ss"
	  };
	
	public static long getDate(String value) throws ParseException{
		Matcher matcher = datePatt.matcher(value);
		long time = 0;
		if(matcher.find()){
			return DateUtil.parseDate(value.length() >= 22 ? value.substring(0,22) : value).getTime();
		}else if((matcher = secondPatt.matcher(value)).find()){
			time = Integer.parseInt(matcher.group(1)) * 1000;
		}else if((matcher = minPatt.matcher(value)).find()){
			time = Integer.parseInt(matcher.group(1)) * 1000 * 60;
		}else if((matcher = hourPatt.matcher(value)).find()){
			time = Integer.parseInt(matcher.group(1)) * 1000 * 60 * 60;
			if((matcher = hourMinPatt.matcher(value)).find()){
				time += Integer.parseInt(matcher.group(2)) * 1000 * 60;
			}
		}else if((matcher = todayPatt.matcher(value)).find()){
			Calendar c = Calendar.getInstance();
			c.set(Calendar.HOUR_OF_DAY, Integer.parseInt(matcher.group(1)));
			c.set(Calendar.MINUTE, Integer.parseInt(matcher.group(2)));
			return c.getTimeInMillis();
		}else if((matcher = todayPattQ.matcher(value)).find()){
			Calendar c = Calendar.getInstance();
			c.set(Calendar.DAY_OF_MONTH, c.get(Calendar.DAY_OF_MONTH) - Integer.parseInt(matcher.group(1)));
			return c.getTimeInMillis();
		}else{
			return DateUtil.parseDate(value).getTime();
		}
		return System.currentTimeMillis() - time;
	}
	
	public static String getHourLater(long hour){
		return sdateFormat.format(System.currentTimeMillis() - hour * 3600 * 1000);
	}
	
	public static long getCurrentYearMillis(){
		Calendar c = Calendar.getInstance();
		c.set(Calendar.MONTH, 0);
		c.set(Calendar.DAY_OF_MONTH, 1);
		c.set(Calendar.HOUR, 8);
		c.set(Calendar.MINUTE, 0);
		return c.getTimeInMillis();
	}
	
	public static Date parseDate(String dateValue) throws ParseException{
		if(dateValue == null)return null;
		for(String format : DATEFORMAT){
			SimpleDateFormat sdf = new SimpleDateFormat(format,Locale.US);
			try {
				Date date = sdf.parse(dateValue);
				return date;
			} catch (ParseException e) {
			}
		}
		return new Date();
	}
	
	public static String formate(long date){
		return sdf.format(new Date(date));
	}
	
	public static int getYear(Date date){
		Calendar c = Calendar.getInstance();
		c.setTime(date);
		return c.get(Calendar.YEAR);
	}
	
	public static int getMonth(Date date){
		Calendar c = Calendar.getInstance();
		c.setTime(date);
		return c.get(Calendar.MONTH);
	}
	
	public static int getDay(Date date){
		Calendar c = Calendar.getInstance();
		c.setTime(date);
		return c.get(Calendar.DAY_OF_MONTH);
	}
}
