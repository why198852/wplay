package com.wplay.hbase.table;

/**
 * HBase?? * @author yaoqiang
 * 
 */
public class HBaseFamilyCloumn {
    private String familyName;
    private String bloomFilter;
    private String compression;
    private boolean inMemory;
    private int version;
    private int replicationScope;

    public String getFamilyName() {
	return familyName;
    }

    public void setFamilyName(String familyName) {
	this.familyName = familyName;
    }

    public String getBloomFilter() {
	return bloomFilter;
    }

    public void setBloomFilter(String bloomFilter) {
	this.bloomFilter = bloomFilter;
    }

    public String getCompression() {
	return compression;
    }

    public void setCompression(String compression) {
	this.compression = compression;
    }

    public boolean isInMemory() {
	return inMemory;
    }

    public void setInMemory(boolean inMemory) {
	this.inMemory = inMemory;
    }

    public int getVersion() {
	return version;
    }

    public void setVersion(int version) {
	this.version = version;
    }

    /**
     * @return the replicationScope
     */
    public int getReplicationScope() {
	return replicationScope;
    }


    /**
     * @param replicationScope
     *            the replicationScope to set
     */
    public void setReplicationScope(int replicationScope) {
	this.replicationScope = replicationScope;
    }

    public String toString() {
	return "HBaseFamilyCloumn [familyName=" + familyName + ", bloomFilter="
		+ bloomFilter + ", compression=" + compression + ", inMemory="
		+ inMemory + ", version=" + version + ", replicationScope="
		+ replicationScope + "]";
    }
}
