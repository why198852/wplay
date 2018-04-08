package com.wplay.spark.streaming.core.parse

import org.apache.spark.Logging

/**
  * Created by james on 16/8/2.
  */
@deprecated(message = "下个版本将移除", since = "1.6.4")
trait Parse[T] extends Logging {

  def doParse(r: String): Option[T]

}
