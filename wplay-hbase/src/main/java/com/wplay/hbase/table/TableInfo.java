package com.wplay.hbase.table;

import java.util.ArrayList;
import java.util.List;

public class TableInfo {

	private String name;//表名
	private String start;
	private String end;
	private boolean split = true;//是否预切分
	private int regions;
	private List<HBaseFamilyCloumn> familys = new ArrayList<HBaseFamilyCloumn>();
	
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public String getStart() {
		return start;
	}
	public void setStart(String start) {
		this.start = start;
	}
	public String getEnd() {
		return end;
	}
	public void setEnd(String end) {
		this.end = end;
	}
	public int getRegions() {
		return regions;
	}
	public void setRegions(int regions) {
		this.regions = regions;
	}
	public void addFamily(HBaseFamilyCloumn family){
		this.familys.add(family);
	}
	public List<HBaseFamilyCloumn> getFamilys(){
		return familys;
	}
	public boolean isSplit() {
		return split;
	}
	public void setSplit(boolean split) {
		this.split = split;
	}
	@Override
	public String toString() {
		return "TableInfo [name=" + name + ",start=" + start + ", end=" + end + ", num=" + regions + ", familys=" + familys + " ]";
	}
}
