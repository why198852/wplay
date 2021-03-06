package com.wplay.spark.streaming.core.format

import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat

/**
  * Created by james on 2016/10/24.
  * 重载
  * generateFileNameForKeyValue 为 s"${key}_$name"
  * generateActualKey 为 null
  */
class InkeTextOutputFormat extends MultipleTextOutputFormat[Any, Any] {

  override def generateFileNameForKeyValue(key: Any, value: Any, name: String): String = {
    s"${key}_$name"
  }

  override def generateActualKey(key: Any, value: Any): AnyRef = {
    null
  }
}
