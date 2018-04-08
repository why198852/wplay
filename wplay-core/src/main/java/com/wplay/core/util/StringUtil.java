package com.wplay.core.util;

import org.apache.log4j.Logger;

import java.io.UnsupportedEncodingException;
import java.lang.reflect.Array;
import java.util.Collection;
import java.util.Map;


public class StringUtil {
	public static final String UTF8_ENCODING = "UTF-8";
	public static final Logger LOG = Logger.getLogger(StringUtil.class);

	public static boolean isEmpty(String src) {
		return (src == null) || (src.trim().isEmpty());
	}

	public static boolean isEmpty(Object obj) {
		if (obj == null)
			return true;
		if (obj.getClass().isArray())
			return Array.getLength(obj) == 0;
		if (obj instanceof Collection<?>)
			return ((Collection<?>) obj).isEmpty();
		if (obj instanceof Map<?, ?>)
			return ((Map<?, ?>) obj).isEmpty();
		return false;
	}

	public static boolean noEmpty(String src) {
		return !src.isEmpty();
	}

	public static String toString(Object src) {
		return isEmpty(src) ? "" : src.toString();
	}

	public static int toInt(String src) {
		if (!isEmpty(src)) {
			return Integer.parseInt(src.trim());
		}
		return 0;
	}

	public static int toInt(Object src) {
		if (!isEmpty(src)) {
			return Integer.parseInt(src.toString().trim());
		}
		return 0;
	}

	public static long toLong(String src) {
		if (!isEmpty(src)) {
			return Long.parseLong(src.trim());
		}
		return 0L;
	}

	public static long toLong(Object src) {
		if (!isEmpty(src)) {
			return toLong(src.toString());
		}
		return 0L;
	}

	public static double toDouble(Object src) {
		if (!isEmpty(src)) {
			return toDouble(src.toString());
		}
		return 0.0D;
	}

	public static double toDouble(String src) {
		if (!isEmpty(src)) {
			return Double.parseDouble(src.trim());
		}
		return 0.0D;
	}

	public static boolean toBoolean(String src) {
		if (!isEmpty(src)) {
			return Boolean.parseBoolean(src);
		}
		return false;
	}

	public static boolean toBoolean(Object src) {
		if (!isEmpty(src)) {
			return toBoolean(src.toString());
		}
		return false;
	}

	public static float toFloat(String src) {
		if (!isEmpty(src)) {
			return Float.parseFloat(src);
		}
		return 0.0F;
	}

	public static float toFloat(Object src) {
		if (!isEmpty(src)) {
			return toFloat(src.toString());
		}
		return 0.0F;
	}

	public static short toShort(String src) {
		if (!isEmpty(src)) {
			return Short.parseShort(src);
		}
		return 0;
	}

	public static short toShort(Object src) {
		if (!isEmpty(src)) {
			return toShort(src.toString());
		}
		return 0;
	}

	public static byte toByte(String src) {
		if (!isEmpty(src)) {
			return Byte.parseByte(src);
		}
		return 0;
	}

	public static byte toByte(Object src) {
		if (!isEmpty(src)) {
			return toByte(src.toString());
		}
		return 0;
	}

	public static char toChar(String src) {
		if (!isEmpty(src)) {
			return src.trim().charAt(0);
		}
		return ' ';
	}

	public static char toChar(Object src) {
		if (!isEmpty(src)) {
			return toChar(src.toString());
		}
		return ' ';
	}

	public static int trimLength(String src) {
		if (src != null) {
			return src.trim().length();
		}
		return 0;
	}

	public static int length(String src) {
		return src.length();
	}

	public static byte[] toBytes(String s) {
		try {
			return s.getBytes("UTF-8");
		} catch (UnsupportedEncodingException e) {
			LOG.error("UTF-8 not supported?", e);
		}
		return null;
	}

	/**
	 * 
	 * @param src
	 * @param dest
	 * @return src equals dest
	 */
	public static boolean equals(String src,String dest){
		if(src == dest){
			return true;
		}else{
			return src != null && src.equals(dest);
		}
	}
	
	/**
	 * 
	 * @param src
	 * @param dest
	 * @return
	 */
	public static int compare(String src,String dest){
		if(src == dest){
			return 0;
		}else if (src == null){
			return -1;
		}else if(dest == null){
			return 1;
		}else{
			return src.compareTo(dest);
		}
	}
}