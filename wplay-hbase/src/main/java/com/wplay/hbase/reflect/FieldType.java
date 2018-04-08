package com.wplay.hbase.reflect;

import java.beans.PropertyDescriptor;
import java.lang.reflect.Type;

/**
 * @author James
 *
 */
public class FieldType {

    private PropertyDescriptor pd;
    private Type type;
    private String name;
    
    /**
     * @return the name
     */
    public String getName() {
        return name;
    }
    /**
     * @param name the name to set
     */
    public void setName(String name) {
        this.name = name;
    }
    public FieldType(){}
    /**
     * @param pd
     * @param type
     */
    public FieldType(PropertyDescriptor pd, Type type) {
	this.pd = pd;
	this.type = type;
    }
    /**
     * @return the pd
     */
    public PropertyDescriptor getPd() {
        return pd;
    }
    /**
     * @param pd the pd to set
     */
    public void setPd(PropertyDescriptor pd) {
        this.pd = pd;
    }
    /**
     * @return the type
     */
    public Type getType() {
        return type;
    }
    /**
     * @param type the type to set
     */
    public void setType(Type type) {
        this.type = type;
    }
    
    
}
