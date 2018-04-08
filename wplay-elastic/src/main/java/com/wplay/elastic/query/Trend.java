package com.wplay.elastic.query;

/**
 * Created by wangy3 on 2017/5/16.
 * <p>
 * Ç÷ÊÆ
 */
public class Trend {
    private String field;
    private String name;
    private String interval;

    public Trend() {
    }

    public Trend(String field, String name, String interval) {
        this.field = field;
        this.name = name;
        this.interval = interval;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getField() {
        return field;
    }

    public void setField(String field) {
        this.field = field;
    }

    public String getInterval() {
        return interval;
    }

    public void setInterval(String interval) {
        this.interval = interval;
    }
}
