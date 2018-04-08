package com.wplay.core.util;

import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class Md5Util {
    private static final char[] HEX_DIGITS = { '0', '1', '2', '3', '4', '5','6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f' };

    public static final String toMd5High(String src){
	try {
	    final MessageDigest digester = MessageDigest.getInstance("MD5");
	    digester.update(src.getBytes());
	    byte[] digest = digester.digest();
	    StringBuilder buf = new StringBuilder(MD5_LEN * 2);
		for (int i = 0; i < MD5_LEN; i++) {
		    int b = digest[i];
		    buf.append(HEX_DIGITS[(b >> 4) & 0xf]);
//		    buf.append(HEX_DIGITS[b & 0xf]);
		}
		return buf.toString();
	} catch (NoSuchAlgorithmException e) {
	    e.printStackTrace();
	    return "";
	}
    }
    
    /**
     * 
     * @param in
     * @param verifyAlgorithm
     * @return
     * @throws IOException
     * @throws NoSuchAlgorithmException
     */
    public static byte[] digest(byte[] in, String verifyAlgorithm)
	    throws IOException, NoSuchAlgorithmException {
	final MessageDigest digester = MessageDigest
		.getInstance(verifyAlgorithm);
	digester.update(in);
	return digester.digest();
    }

    public static final int MD5_LEN = 16;



    public static final String toMd5String(byte[] digest) {
	StringBuilder buf = new StringBuilder(MD5_LEN * 2);
	for (int i = 0; i < MD5_LEN; i++) {
	    int b = digest[i];
	    buf.append(HEX_DIGITS[(b >> 4) & 0xf]);
	    buf.append(HEX_DIGITS[b & 0xf]);
	}
	return buf.toString();
    }

}
