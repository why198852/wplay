package com.wplay.elastic.query;

/**
 * Created by wangy3 on 2016/11/23.
 * <p>
 * ²éÑ¯²Ù×÷
 */
public class QueryOper {

    private String field;
    private String oper;
    private Object val;
    private Object val2;
    private int step;

    public QueryOper() {
    }

    public QueryOper(String field, String oper, Object val) {
        this.field = field;
        this.oper = oper;
        this.val = val;
    }

    public QueryOper(String field, String oper) {
        this.field = field;
        this.oper = oper;
    }

    public QueryOper(String field, String oper, Object val, Object val2) {
        this.field = field;
        this.oper = oper;
        this.val = val;
        this.val2 = val2;
    }

    public QueryOper(String field, String oper, Object val, Object val2,int step) {
        this.field = field;
        this.oper = oper;
        this.val = val;
        this.val2 = val2;
        this.step=step;
    }

    public int getStep() {
        return step;
    }

    public void setStep(int step) {
        this.step = step;
    }

    public String getField() {
        return field;
    }

    public void setField(String field) {
        this.field = field;
    }

    public String getOper() {
        return oper;
    }

    public void setOper(String oper) {
        this.oper = oper;
    }

    public Object getVal() {
        return val;
    }

    public void setVal(Object val) {
        this.val = val;
    }

    public Object getVal2() {
        return val2;
    }

    public void setVal2(Object val2) {
        this.val2 = val2;
    }
}
