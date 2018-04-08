/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.wplay.spark.streaming.core.ip;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.Charset;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author james
 */
public final class IP {


    private static ConcurrentHashMap<String, IpInfo> IPCACHE = new ConcurrentHashMap<String, IpInfo>();

    public static IP init() {
        Long st = System.nanoTime();
        IP ip = new IP();
        Long et = System.nanoTime();

        return ip;
    }

    private int offset;
    private static byte[] IPDB_FILE_CONTENT = null;
    private final int[] index = new int[65536];
    private ByteBuffer dataBuffer;
    private ByteBuffer indexBuffer;
    private final ReentrantLock lock = new ReentrantLock();

    public static boolean isboolIp(String ipAddress) {
        String ip = "([1-9]|[1-9]\\d|1\\d{2}|2[0-4]\\d|25[0-5])(\\.(\\d|[1-9]\\d|1\\d{2}|2[0-4]\\d|25[0-5])){3}";
        Pattern pattern = Pattern.compile(ip);
        Matcher matcher = pattern.matcher(ipAddress);
        return matcher.matches();
    }

    private IP() {
        Config config = ConfigFactory.load();
        String ipDataPath = config.getString("spark.ip.data.path");
        if (IPDB_FILE_CONTENT == null) {
            IPDB_FILE_CONTENT = readHDFSFile(ipDataPath);
        }
        loadToTree();
    }

    /*
     * read the hdfs file content
     * notice that the dst is the full path name
     */
    private byte[] readHDFSFile(String dst) {
        try {
            Configuration conf = new Configuration();
            FileSystem fs = FileSystem.get(conf);
            // check if the file exists
            Path path = new Path(dst);
            if (fs.exists(path)) {
                byte[] buffer;
                // get the file info to create the buffer
                try (FSDataInputStream is = fs.open(path)) {
                    // get the file info to create the buffer
                    FileStatus stat = fs.getFileStatus(path);
                    // create the buffer
                    buffer = new byte[Integer.parseInt(String.valueOf(stat.getLen()))];
                    is.readFully(0, buffer);
                }
                return buffer;
            }
        } catch (IOException ex) {
            ex.printStackTrace();
        }
        return null;
    }

    public String[] find(String ip) {
        String[] ips = ip.split("\\.");
        int prefix_value = (Integer.valueOf(ips[0]) * 256 + Integer.valueOf(ips[1]));
        long ip2long_value = ip2long(ip);
        int start = index[prefix_value];
        int max_comp_len = offset - 262144 - 4;
        long tmpInt;
        long index_offset = -1;
        int index_length = -1;
        byte b = 0;
        int loop = 0;
        for (start = start * 9 + 262144; start < max_comp_len; start += 9) {
            tmpInt = int2long(indexBuffer.getInt(start));
            loop = loop + 1;
            if (tmpInt >= ip2long_value) {
                index_offset = bytesToLong(b, indexBuffer.get(start + 6), indexBuffer.get(start + 5), indexBuffer.get(start + 4));
                index_length = (0xFF & indexBuffer.get(start + 7) << 8) + (0xFF & indexBuffer.get(start + 8));
                break;
            }
        }
        byte[] areaBytes;

        lock.lock();
        try {
            dataBuffer.position(offset + (int) index_offset - 262144);
            areaBytes = new byte[index_length];
            dataBuffer.get(areaBytes, 0, index_length);
        } finally {
            lock.unlock();
        }

        return new String(areaBytes, Charset.forName("UTF-8")).split("\t");
    }

    public IpInfo getIpInfo(String ip) {
        IpInfo ipInfo = null;
        if (IPCACHE.containsKey(ip)) {
            ipInfo = IPCACHE.get(ip);
        } else if (ip.contains(".")) {
            String dd = "";
            lock.lock();
            try {
                String[] ips = ip.split("\\.");
                int prefix_value = (Integer.valueOf(ips[0]) * 256 + Integer.valueOf(ips[1]));
                long ip2long_value = ip2long(ip);
                int start = index[prefix_value];
                int max_comp_len = offset - 262144 - 4;
                long tmpInt;
                long index_offset = -1;
                int index_length = -1;
                byte b = 0;
                int loop = 0;
                for (start = start * 9 + 262144; start < max_comp_len; start += 9) {
                    tmpInt = int2long(indexBuffer.getInt(start));
                    loop = loop + 1;
                    if (tmpInt >= ip2long_value) {
                        index_offset = bytesToLong(b, indexBuffer.get(start + 6), indexBuffer.get(start + 5), indexBuffer.get(start + 4));
                        index_length = (0xFF & indexBuffer.get(start + 7) << 8) + (0xFF & indexBuffer.get(start + 8));
                        break;
                    }
                }

                dataBuffer.position(offset + (int) index_offset - 262144);
                byte[] areaBytes = new byte[index_length];
                dataBuffer.get(areaBytes, 0, index_length);
                dd = new String(areaBytes, Charset.forName("UTF-8"));
                String[] datArr = dd.split("\t", -1);
                // [中国 天津 天津 * 鹏博士 39.125596 117.190182 Asia/Shanghai UTC+8 120000 86 CN AP]
                ipInfo = new IpInfo(datArr[0], datArr[1], datArr[2], datArr[4], datArr[6], datArr[5]);
                IPCACHE.put(ip, ipInfo);
            } finally {
                lock.unlock();
            }

        } else {
            return new IpInfo();
        }
        return ipInfo;
    }

    private void loadToTree() {
        lock.lock();
        try {
            if (IPDB_FILE_CONTENT != null) {
                dataBuffer = ByteBuffer.allocate(Long.valueOf(IPDB_FILE_CONTENT.length).intValue());
                dataBuffer.put(IPDB_FILE_CONTENT);
                dataBuffer.position(0);
                int indexLength = dataBuffer.getInt();
                byte[] indexBytes = new byte[indexLength];
                dataBuffer.get(indexBytes, 0, indexLength - 4);
                indexBuffer = ByteBuffer.wrap(indexBytes);
                indexBuffer.order(ByteOrder.LITTLE_ENDIAN);
                offset = indexLength;

                for (int i = 0; i < 256; i++) {
                    for (int j = 0; j < 256; j++) {
                        int v = indexBuffer.getInt();
                        index[i * 256 + j] = v;
                    }
                }
                indexBuffer.order(ByteOrder.BIG_ENDIAN);
            }
        } finally {
            lock.unlock();
        }
    }

    private static long bytesToLong(byte a, byte b, byte c, byte d) {
        return int2long((((a & 0xff) << 24) | ((b & 0xff) << 16) | ((c & 0xff) << 8) | (d & 0xff)));
    }

    private static int str2Ip(String ip) {
        String[] ss = ip.split("\\.");
        int a, b, c, d;
        a = Integer.parseInt(ss[0]);
        b = Integer.parseInt(ss[1]);
        c = Integer.parseInt(ss[2]);
        d = Integer.parseInt(ss[3]);
        return (a << 24) | (b << 16) | (c << 8) | d;
    }

    private static long ip2long(String ip) {
        return int2long(str2Ip(ip));
    }

    private static long int2long(int i) {
        long l = i & 0x7fffffffL;
        if (i < 0) {
            l |= 0x080000000L;
        }
        return l;
    }
}
