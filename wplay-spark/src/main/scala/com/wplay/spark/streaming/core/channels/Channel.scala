package com.wplay.spark.streaming.core.channels

import org.apache.spark.streaming.dstream.DStream

/**
  * Created by james on 16/8/2.
  */
trait Channel[M, N] {

  /**
    * 处理
    *
    * @param dStream
    * @return
    */
  def procese(dStream: DStream[M]): DStream[N]
}
