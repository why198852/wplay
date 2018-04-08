package com.wplay.core.vo;

import org.w3c.dom.Element;

public interface XmlAble {
	
	/**
	 * 把xml 解析为对象
	 * @param node
	 * @return
	 */
	public void parse(Element node);

	/**
	 * 对象解析为xml
	 * @return
	 */
	public Element toNode();
	
	
}
