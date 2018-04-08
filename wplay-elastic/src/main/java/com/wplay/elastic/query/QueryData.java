package com.wplay.elastic.query;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Created by wangy3 on 2016/11/25.
 * 引擎
 */
public class QueryData {

    // ES index
    private String index = "";
    // ES type
    private String type = "";
    // 报表类型  日报|周报|月报|全量
    private String reportType = "";
    // 统计类型 eg：详情，数量，求和，平均值 等等
    private String stat = "";
    // 分页查询开始位置
    private int from = 0;
    // 默认单次查询输出条数
    private int size = 100;


    // 请求超时时间
    private long timeout = 0;
    // 趋势
    private Trend trend;


    // 分组字段，多个分组用 逗号 分割
    private List<String> group = new ArrayList<>();
    // 前台选择展示字段
    private Set<String> select = new HashSet<>();

    private String metaRoleId = "";

    // 与 或 运算，
    private String andOr = "and";
    // where 过滤条件
    private List<QueryOper> where = new ArrayList<>();

    private QueryStep step = null;

    public List<String> getSort() {
        return sort;
    }

    public void setSort(List<String> sort) {
        this.sort = sort;
    }

    //排序字段，多个字段用逗号分割
    private List<String> sort;

    public QueryStep getStep() {
        return step;
    }

    public void setStep(QueryStep step) {
        this.step = step;
    }

    public String getOrderby() {
        return orderby;
    }

    public void setOrderby(String orderby) {
        this.orderby = orderby;
    }

    private String orderby;

    public String getIndex() {
        return index;
    }

    public void setIndex(String index) {
        this.index = index;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getReportType() {
        return reportType;
    }

    public void setReportType(String reportType) {
        this.reportType = reportType;
    }

    public String getStat() {
        return stat;
    }

    public void setStat(String stat) {
        this.stat = stat;
    }

    public int getFrom() {
        return from;
    }

    public void setFrom(int from) {
        this.from = from;
    }

    public int getSize() {
        return size;
    }

    public void setSize(int size) {
        this.size = size;
    }

    public List<String> getGroup() {
        return group;
    }

    public void setGroup(List<String> group) {
        this.group = group;
    }

    public Set<String> getSelect() {
        return select;
    }

    public void setSelect(Set<String> select) {
        this.select = select;
    }

    public String getAndOr() {
        return andOr;
    }

    public void setAndOr(String andOr) {
        this.andOr = andOr;
    }

    public List<QueryOper> getWhere() {
        return where;
    }

    public void setWhere(List<QueryOper> where) {
        this.where = where;
    }

    public String getMetaRoleId() {
        return metaRoleId;
    }

    public void setMetaRoleId(String metaRoleId) {
        this.metaRoleId = metaRoleId;
    }

    public Trend getTrend() {
        return trend;
    }

    public void setTrend(Trend trend) {
        this.trend = trend;
    }

    public long getTimeout() {
        return timeout;
    }

    public void setTimeout(long timeout) {
        this.timeout = timeout;
    }
}
