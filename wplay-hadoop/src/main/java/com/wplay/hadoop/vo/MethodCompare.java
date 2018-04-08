package com.wplay.hadoop.vo;

/**
 * 
 * @author James
 *
 */
class MethodCompare implements Comparable<MethodCompare>{

	private int num;
	private Object value;
	private Class<?> type;
	
	public MethodCompare(){
	}
	
	/**
	 * 
	 * @param num
	 * @param value
	 */
	public MethodCompare(int num, Object value) {
		this.num = num;
		this.value = value;
	}

	/**
	 * 
	 * @param num
	 * @param type
	 */
	public MethodCompare(int num, Class<?> type) {
		this.num = num;
		this.type = type;
	}
	
	public Class<?> getType() {
		return type;
	}

	public void setType(Class<?> type) {
		this.type = type;
	}

	public int getNum() {
		return num;
	}

	public void setNum(int num) {
		this.num = num;
	}

	public Object getValue() {
		return value;
	}

	public void setValue(Object value) {
		this.value = value;
	}

	@Override
	public int compareTo(MethodCompare o) {
		return num - o.num;
	}
}
