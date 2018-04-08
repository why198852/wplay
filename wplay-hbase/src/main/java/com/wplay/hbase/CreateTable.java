package com.wplay.hbase;

import com.wplay.hbase.table.HBaseFamilyCloumn;
import com.wplay.hbase.table.TableInfo;
import com.wplay.hbase.table.TableUtil;
import com.wplay.core.util.XmlConfigUtil;
import com.wplay.core.util.StringUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.io.compress.Compression.Algorithm;
import org.apache.hadoop.hbase.regionserver.BloomType;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.math.BigInteger;
import java.util.Collection;
import java.util.regex.Pattern;

public class CreateTable {
    private static final Pattern pattern = Pattern.compile(
	    "create|recreate|delete", Pattern.CASE_INSENSITIVE);
    private static final Pattern create = Pattern.compile("create|recreate",
	    Pattern.CASE_INSENSITIVE);
    private static final Pattern delete = Pattern.compile("recreate|delete",
	    Pattern.CASE_INSENSITIVE);

    private static final Logger LOG = Logger.getLogger(CreateTable.class);

    /**
	 * 
	 */
    private static final void printUsage() {
	String usage = "Usage : CreateTable <create <all|tableName>|recreate <all|tableName>|delete <all|tableName>>";
	System.out.println(usage);
	System.exit(-1);
    }

    public static void main(String[] args) throws Exception {
	if (args.length < 2) {
	    printUsage();
	}
	String command = args[0];
	if (isCommand(command)) {
	    int delete = 0;
	    int create = 0;
	    String type = args[1];
	    boolean del = isDelete(command);
	    boolean cre = isCreate(command);
	    Configuration conf = XmlConfigUtil.create();
	    HBaseAdmin admin = new HBaseAdmin(conf);
	    Collection<TableInfo> tables = TableUtil.getAllTables();
	    for (TableInfo tableInfo : tables) {
			if (del) {
				boolean delFalg = false;
				if(type.equals("all")){
					delFalg = true;
				}else if(tableInfo.getName().equalsIgnoreCase(type)){
					delFalg = true;
				}
				if(delFalg){
					if (deleteTable(admin, tableInfo)) {
						delete++;
					}
				}
			}
			if (cre) {
				boolean creFalg = false;
				if(type.equals("all")){
					creFalg = true;
				}else if(tableInfo.getName().equalsIgnoreCase(type)){
					creFalg = true;
				}
				if(creFalg){
					if (createTable(admin, tableInfo)) {
				    	create++;
				    }	
				}
			}
	    }

	    admin.close();
	    LOG.info("共删除表 " + delete + " 个");
	    LOG.info("共创建表 " + create + " 个");
	} else {
	    printUsage();
	}
    }

    private static final boolean isCommand(String command) {
	return pattern.matcher(command).matches();
    }

    private static final boolean isDelete(String command) {
	return delete.matcher(command).matches();
    }

    private static final boolean isCreate(String command) {
	return create.matcher(command).matches();
    }

    /**
     * 删除表
     *
     * @param admin
     * @param tableInfo
     * @throws IOException
     */
    private static final boolean deleteTable(HBaseAdmin admin,
	    TableInfo tableInfo) throws IOException {
	String tableName = tableInfo.getName();
	if (admin.tableExists(tableName)) {
	    LOG.info("Will delete table " + tableName);
	    if (admin.isTableEnabled(tableName)) {
		admin.disableTable(tableName);
	    }
	    admin.deleteTable(tableName);
	    LOG.info("Delete table " + tableName + " success");
	    return true;
	} else {
	    LOG.warn("Table " + tableName + " not exist!");
	    return false;
	}
    }


    private static byte[][] getHexSplits(String startKey, String endKey,
	    int numRegions) {
	byte[][] splits = new byte[numRegions - 1][];
	try {
	    BigInteger lowestKey = new BigInteger(startKey, 16);
	    BigInteger highestKey = new BigInteger(endKey, 16);
	    BigInteger range = highestKey.subtract(lowestKey);
	    BigInteger regionIncrement = range.divide(BigInteger
		    .valueOf(numRegions));
	    lowestKey = lowestKey.add(regionIncrement);
	    for (int i = 0; i < numRegions - 1; i++) {
		BigInteger key = lowestKey.add(regionIncrement
			.multiply(BigInteger.valueOf(i)));
		byte[] b = String.format("%016x", key).getBytes();
		splits[i] = b;
	    }
	} catch (Exception e) {
	    byte[] startRow = Bytes.toBytes(startKey);
	    byte[] endRow = Bytes.toBytes(endKey);
	    splits = Bytes.split(startRow, endRow, numRegions);
	}
	return splits;
    }

    /**
     * 创建表
     * 
     * @param admin
     * @param tableInfo
     * @throws IOException
     */
    private static final boolean createTable(HBaseAdmin admin,
	    TableInfo tableInfo) throws IOException {
	String tableName = tableInfo.getName();
	if (!admin.tableExists(tableName)) {
	    LOG.info("Will create table " + tableName);
	    HTableDescriptor tableDescripter = new HTableDescriptor(tableName);
	    for (HBaseFamilyCloumn fam : tableInfo.getFamilys()) {
		HColumnDescriptor hcolumn = new HColumnDescriptor(
			Bytes.toBytes(fam.getFamilyName()));
		if (!StringUtil.isEmpty(fam.getCompression())) {
		    Algorithm compressType = Algorithm.valueOf(fam
			    .getCompression());
		    if (compressType != null) {
			hcolumn.setCompressionType(compressType);
		    }
		}
		if (!StringUtil.isEmpty(fam.getBloomFilter())) {
		    BloomType bloomType = BloomType.valueOf(fam
			    .getBloomFilter());
		    if (bloomType != null) {
			hcolumn.setBloomFilterType(bloomType);
		    }
		}
		hcolumn.setInMemory(fam.isInMemory());
		if (fam.getVersion() > 0) {
		    hcolumn.setMaxVersions(fam.getVersion());
		}
		if (fam.getReplicationScope() > 0) {
		    hcolumn.setScope(fam.getReplicationScope());
		}
		tableDescripter.addFamily(hcolumn);
	    }

	    if (tableInfo.isSplit()) {
		byte[][] splists = getHexSplits(tableInfo.getStart(),
			tableInfo.getEnd(), tableInfo.getRegions());
		admin.createTable(tableDescripter, splists);
	    } else {
		admin.createTable(tableDescripter);
	    }
	    LOG.info("Create table " + tableName + " success!");
	    return true;
	} else {
	    LOG.warn("Table " + tableName + " already exist!");
	    return false;
	}
    }
}
