/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.wplay.spark.streaming.core.ip;

public class IpInfo {

    private int start;
    private int end;
    private String code;
    private String city;
    private String isp;
    private String country;
    private String province;
    private String longitude;
    private String latitude;

    public IpInfo(int start, int end, String country, String province, String city, String isp) {
        this.start = start;
        this.end = end;
        this.city = city;
        this.isp = isp;
        this.country = country;
        this.province = province;
    }

    public IpInfo(String country, String province, String city, String isp, String longitude, String latitude) {
        this.country = country;
        this.province = province;
        this.city = city;
        this.isp = isp;
        this.longitude = longitude;
        this.latitude = latitude;
    }

    public IpInfo() {
        this.start = -1;
        this.end = -1;
        this.city = "未知";
        this.isp = "未知";
        this.country = "未知";
        this.province = "未知";
        this.longitude = "未知";
        this.latitude = "未知";
    }

    public int getStart() {
        return start;
    }

    public void setStart(int start) {
        this.start = start;
    }

    public int getEnd() {
        return end;
    }

    public void setEnd(int end) {
        this.end = end;
    }

    public String getCountry() {
        return (country == null || country.isEmpty()) ? "未知" : country;
    }

    public void setCountry(String country) {
        this.country = country;
    }

    public String getCode() {
        return code;
    }

    public void setCode(String code) {
        this.code = code;
    }

    public String getCity() {
        return (city == null || city.isEmpty()) ? "未知" : city;
    }

    public void setCity(String city) {
        this.city = city;
    }

    public String getIsp() {
        return (isp == null || isp.isEmpty()) ? "未知" : isp;
    }

    public void setIsp(String isp) {
        this.isp = isp;
    }

    public String getProvince() {
        return (province == null || province.isEmpty()) ? "未知" : province;
    }

    public void setProvince(String province) {
        this.province = province;
    }

    public String getLongitude() {return this.longitude;}

    public void setLongitude(String longitude) {this.longitude = longitude;}

    public String getLatitude() {return this.latitude;}

    public void setLatitude(String latitude) {this.latitude = latitude;}

    @Override
    public String toString() {
        return "IpInfo{" + "country=" + country + ", province=" + province + ", city=" + city + ", isp=" + isp + '}';
    }

}
