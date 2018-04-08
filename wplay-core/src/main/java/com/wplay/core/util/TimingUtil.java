package com.wplay.core.util;

import java.text.NumberFormat;

public class TimingUtil {
	private static long[] TIME_FACTOR = { 3600000L, 60000L, 1000L,1L};

	public static String elapsedTime(long start, long end) {
		if (start > end) {
			return null;
		}

		long[] elapsedTime = new long[TIME_FACTOR.length];

		for (int i = 0; i < TIME_FACTOR.length; i++) {
			elapsedTime[i] = (start > end ? -1L : (end - start)
					/ TIME_FACTOR[i]);
			start += TIME_FACTOR[i] * elapsedTime[i];
		}

		NumberFormat nf = NumberFormat.getInstance();
		nf.setMinimumIntegerDigits(2);
		StringBuffer buf = new StringBuffer();
		for (int i = 0; i < elapsedTime.length; i++) {
			if (i > 0) {
				buf.append(":");
			}
			buf.append(nf.format(elapsedTime[i]));
		}
		return buf.toString();
	}
}