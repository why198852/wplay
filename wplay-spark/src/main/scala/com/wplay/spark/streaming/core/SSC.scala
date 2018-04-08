package com.wplay.spark.streaming.core

import com.wplay.core.util.Utils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.streaming.{CongestionMonitorListener, InkeStatsReportListener, Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by james on 16/8/4.
  * 封装Spark Streaming
  *
  */

@deprecated(message = "下个版本将移除,请使用 InkeStreaming", since = "1.6.4")
object SSC {

  /**
    *
    * @param initFun        初始化方法
    * @param toDoFun        执行逻辑
    * @param monitor        是否本启用监控
    * @param checkpointPath checkpoint 目录,为空表示不需要 checkpoint
    * @param isLocal        是否本地运行  方便测试
    */
  def apply(initFun: SparkConf => Unit = (_) => {},
            toDoFun: StreamingContext => Unit,
            monitor: Boolean = false,
            checkpointPath: String = "",
            isLocal: Boolean = false): Unit = {

    def creatingFunc: () => StreamingContext = {
      () => {

        val sparkConf = new SparkConf()

        initFun(sparkConf)

        if (isLocal) {
          Logger.getRootLogger.setLevel(Level.ERROR)
          sparkConf.setAppName("SSCTest").setMaster("local[*]")
          sparkConf.set("spark.streaming.kafka.maxRatePerPartition", "10")
          // 约定传入此参数,则表示本地 Debug
          if (sparkConf.contains("spark.properties.file")) {
            sparkConf.setAll(Utils.getPropertiesFromFile(sparkConf.get("spark.properties.file")))
          }
        }
        // 时间间隔
        val slide = sparkConf.get("spark.slide", "5").toInt
        val sc = new SparkContext(sparkConf)
        val ssc = new StreamingContext(sc, Seconds(slide))

        // 拥堵告警
        ssc.addStreamingListener(new CongestionMonitorListener(ssc))

        if (monitor) ssc.addStreamingListener(new InkeStatsReportListener(ssc))

        toDoFun(ssc)
        ssc
      }
    }

    val context = {
      if (checkpointPath.isEmpty) {
        creatingFunc()
      } else {

        val ssc = StreamingContext.getOrCreate(checkpointPath, creatingFunc, createOnError = true)

        ssc.checkpoint(checkpointPath)
        ssc
      }
    }

    context.start()
    context.awaitTermination()
  }
}
