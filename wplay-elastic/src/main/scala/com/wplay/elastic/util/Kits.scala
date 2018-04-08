package com.wplay.elastic.util

import java.util

object Kits {

  def hash(s: String): String = {
    val m = java.security.MessageDigest.getInstance("MD5")
    val b = s.getBytes("UTF-8")
    m.update(b, 0, b.length)
    new java.math.BigInteger(1, m.digest()).toString(16)
  }


  def isEmpty(any: Any): Boolean = {
    any match {
      case null | None | "" => true
      case iter: Iterator[_] => iter.isEmpty
      case list: util.Collection[_] => list.isEmpty
    }
  }


  def main(args: Array[String]): Unit = {

    val log = """{"command":"./modules/spark-server-1.0/submit.sh","args":["conf/online/sparksql.properties"],"body":Map(sql -> select ymd,count(distinct uid)  as dd,  count(distinct case when client = 'android' then uid end) as dau_android, count(distinct case when client = 'ios' then uid end) as dau_ios, count(distinct case when ropklv = 1 then uid end) as ios_other from hdw.u_user_active_all_v2 where ymd >= '20171223' group by ymd , model -> overwrite, key -> 673103fc990f1954eac38579bd2e240d)}"""


  }


}
