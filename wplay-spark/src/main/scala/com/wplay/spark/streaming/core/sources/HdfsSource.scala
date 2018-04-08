package com.wplay.spark.streaming.core.sources

import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream

/**
  * Created by james on 16/8/26.
  *
  * 读取HDFS数据
  *
  * @param ssc
  * @param path 读取路径
  */
class HdfsSource(@transient
                 val ssc: StreamingContext,
                 path: String)
  extends Source[String] {

  /**
    * 获取DStream 流
    *
    * @return
    */
  override def getDStream: DStream[String] = {
    ssc.textFileStream(path)
  }
}
