package com.wplay.core.util;

import java.io.UnsupportedEncodingException;

/**
 * 
 * @author James
 *
 */
public class EnCodeUtil {

	/**
	 * »ñÈ¡±àÂëºÅµÄ×Ö·û´®
	 * @param b
	 * @param len
	 * @param charset
	 * @return
	 * @throws UnsupportedEncodingException
	 */
	public static final String getStr(byte[] b,int len,String charset) throws UnsupportedEncodingException{
		return new String(b, 0,len, charset);
	}
}
