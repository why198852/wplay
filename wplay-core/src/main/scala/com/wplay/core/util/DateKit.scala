package com.wplay.core.util

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

/**
  *
  * Created by james on 16/7/25.
  *
  * 日期工具包
  */
object DateKit {

  val yyyyMMdd = new SimpleDateFormat("yyyMMdd")

  /**
    * long 转换成 yyyMMdd 的时间格式
    *
    * @param long
    * @return
    */
  def to_yyyyMMdd(long: Long): String = {
    yyyyMMdd.format(new Date(long))
  }

  /**
    * 字符串转换 yyyMMdd 的时间格式
    *
    * @param str
    * @return 不合规数据返回 00000000
    */
  def to_yyyyMMdd(str: String): String = {
    try {
      to_yyyyMMdd(str.toLong)
    } catch {
      case e: Exception => "00000000"
    }
  }


  // 获得当天24点时间
  def getTimesnight: Date = {
    val cal = Calendar.getInstance()
    cal.set(Calendar.HOUR_OF_DAY, 24)
    cal.set(Calendar.SECOND, 0)
    cal.set(Calendar.MINUTE, 0)
    cal.set(Calendar.MILLISECOND, 0)
    cal.getTime
  }


  /**
    * 多少天以前
    *
    * @param date
    * @param day
    * @return
    */
  def isDayAgo(date: Date, day: Int): Boolean = {
    date.before(getDayAgoFromNow(day))
  }


  /**
    * 获得今天零点日期
    *
    * @return
    */
  def getTimesmorning: Date = {
    val cal = Calendar.getInstance()
    cal.set(Calendar.HOUR_OF_DAY, 0)
    cal.set(Calendar.SECOND, 0)
    cal.set(Calendar.MINUTE, 0)
    cal.set(Calendar.MILLISECOND, 0)
    cal.getTime
  }


  /**
    * 获得当前N 天前日期
    *
    * @param day
    * @return
    */
  def getDayAgoFromNow(day: Int): Date = {
    val cal = Calendar.getInstance()
    cal.setTimeInMillis(getTimesnight.getTime - 3600 * 24 * 1000 * day)
    cal.getTime
  }

  /**
    * 获取七天前日期
    *
    * @return
    */
  def get7DayAgoFromNow: Date = {
    getDayAgoFromNow(7)
  }

  /**
    * 上周
    *
    * @return
    */
  def getWeekFromNow: Date = {
    val cal = Calendar.getInstance()
    cal.setTimeInMillis(getTimesmorning.getTime - 3600 * 24 * 1000 * 7)
    cal.getTime
  }


  def main(args: Array[String]) {
    println(DateKit.yyyyMMdd.format(new Date()))
  }
}
