package com.wplay.spark.streaming.core.kit

import com.wplay.spark.streaming.core.ip.{IP, IpInfo}
import org.apache.spark.Logging

/**
  * Created by james on 16/9/20.
  *
  * IP 解析工具
  */
object IpKit extends Logging {

  private lazy val ipi = IP.init()

  private val ipR = """((?:(?:25[0-5]|2[0-4]\d|((1\d{2})|([1-9]?\d)))\.){3}(?:25[0-5]|2[0-4]\d|((1\d{2})|([1-9]?\d))))""".r

  def getIpInfo(ipAddr: String): IpInfo = {

    ipR.findFirstIn(ipAddr) match {

      case Some(_ip) => ipi.getIpInfo(_ip)
      case _ => logWarning(s"IP Parse error [$ipAddr]")
        new IpInfo()
    }
  }

  def main(args: Array[String]): Unit = {
    println(ipR.findFirstIn("122.156.e.2"))
  }

}
