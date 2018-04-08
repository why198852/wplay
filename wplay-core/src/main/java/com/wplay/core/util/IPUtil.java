package com.wplay.core.util;

import java.util.Random;

public class IPUtil {

    /**
     * 将127.0.0.1形式的IP地址转换成十进制整数，这里没有进行任何错误处理
     * 通过左移位操作（<<）给每一段的数字加权，第一段的权为2的24次方，第二段的权为2的16次方，第三段的权为2的8次方，最后一段的权为1
     */
    public static long ipToLong(String ipaddress) {
	long[] ip = new long[4];
	// 先找到IP地址字符串中.的位置
	int position1 = ipaddress.indexOf(".");
	int position2 = ipaddress.indexOf(".", position1 + 1);
	int position3 = ipaddress.indexOf(".", position2 + 1);
	// 将每个.之间的字符串转换成整型
	ip[0] = Long.parseLong(ipaddress.substring(0, position1));
	ip[1] = Long.parseLong(ipaddress.substring(position1 + 1, position2));
	ip[2] = Long.parseLong(ipaddress.substring(position2 + 1, position3));
	ip[3] = Long.parseLong(ipaddress.substring(position3 + 1));
	return (ip[0] << 24) + (ip[1] << 16) + (ip[2] << 8) + ip[3];
    }

    /**
     * 将十进制整数形式转换成127.0.0.1形式的ip地址 将整数值进行右移位操作（>>>），右移24位，右移时高位补0，得到的数字即为第一段IP。
     * 通过与操作符（&）将整数值的高8位设为0，再右移16位，得到的数字即为第二段IP。
     * 通过与操作符吧整数值的高16位设为0，再右移8位，得到的数字即为第三段IP。 通过与操作符吧整数值的高24位设为0，得到的数字即为第四段IP。
     */
    public static String longToIP(long ipaddress) {
	StringBuffer sb = new StringBuffer("");
	// 直接右移24位
	sb.append(String.valueOf((ipaddress >>> 24)));
	sb.append(".");
	// 将高8位置0，然后右移16位
	sb.append(String.valueOf((ipaddress & 0x00FFFFFF) >>> 16));
	sb.append(".");
	// 将高16位置0，然后右移8位
	sb.append(String.valueOf((ipaddress & 0x0000FFFF) >>> 8));
	sb.append(".");
	// 将高24位置0
	sb.append(String.valueOf((ipaddress & 0x000000FF)));
	return sb.toString();
    }
    
    private static final Random ran = new Random();
    
    /**
     * 
     * @return
     */
    public static final String randomIp(){
	String _1 = Integer.toString(ran.nextInt(256));
	String _2 = Integer.toString(ran.nextInt(256));
	String _3 = Integer.toString(ran.nextInt(256));
	String _4 = Integer.toString(ran.nextInt(256));
	return _1 + "." + _2 + "." + _3 +"." + _4;
    }

    /**
     * 
     * @return
     */
    public static final int randomPart(){
	return ran.nextInt(65536);
    }
    
    public static final float randomFloat(){
	return ran.nextFloat() * 10000;
    }
    
    private static final char[] CHARS = { '0', '1', '2', '3', '4', '5','6', '7', '8', '9'
			,'a','b','c','d','e','f','g','h','i','j','k','l','m','n','o','p','q','r','s','t','u','v','w','x','y','z'
			,'A','B','C','D','E','F','G','H','I','J','K','L','M','N','O','P','Q','R','S','T','U','V','W','X','Y','Z' };
    
    public static final String randomStr(){
	int len = ran.nextInt(30) + 1;
	StringBuilder sb = new StringBuilder();
	for(int i = 0;i < len;i ++){
	    sb.append(CHARS[ran.nextInt(CHARS.length)]);
	}
	return sb.toString();
    }
    
    public static final boolean hasB(){
	return ran.nextInt(4) == 0;
    }
    
    public static void main(String[] args) {
//	System.out.println(randomIp());
//	for(char i = 'A';i <= 'Z'; i++){
//	    System.out.print("'" + i + "'" + ",");
//	}
//	System.out.println(CHARS.length);
	System.out.println(randomFloat());
    }
}
