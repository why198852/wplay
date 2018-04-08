package com.wplay.elastic.query;

/**
 * Created by wangy3 on 2017/3/31.
 */
public class QueryStep {


    private String field;
    private String val;


    public String getField() {
        return field;
    }

    public void setField(String field) {
        this.field = field;
    }

    public String getVal() {
        return val;
    }

    public void setVal(String val) {
        this.val = val;
    }

    public QueryStep(){

    }

    public QueryStep(String field, String val) {
        this.field = field;
        this.val = val;
    }


    public void info(){
        String str="0-10,10-30,30-70";
        for(String s:str.split(",")){
            System.out.println(Integer.parseInt(s.split("-")[0])+","+s.split("-")[1]);
        }
    }
    }
