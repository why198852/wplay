package com.wplay.spark.streaming.core

import com.wplay.core.util.Utils
import org.apache.spark.streaming.{CongestionMonitorListener, InkeStatsReportListener, Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by james on 2017/4/26.
  *
  * Wplay Spark Streaming 入口封装
  *
  */
trait WplayStreaming {


  protected final def args: Array[String] = _args

  private final var _args: Array[String] = _


  // 是否开启监控
  private var monitor: Boolean = false

  // checkpoint目录
  private var checkpointPath: String = ""

  // 从checkpoint 中恢复失败，则重新创建
  private var createOnError: Boolean = true

  /**
    * 初始化，函数，可以设置 sparkConf
    *
    * @param sparkConf
    */
  def init(sparkConf: SparkConf): Unit = {}

  /**
    * 处理函数
    *
    * @param ssc
    */
  def handle(ssc: StreamingContext): Unit


  /**
    * 创建 Context
    *
    * @return
    */
  def creatingContext(): StreamingContext = {

    val sparkConf = new SparkConf()

    // 约定传入此参数,则表示本地 Debug
    if (sparkConf.contains("spark.properties.file")) {
      sparkConf.setAppName("LocalDebug").setMaster("local[*]")
      sparkConf.set("spark.streaming.kafka.maxRatePerPartition", "10")
      sparkConf.setAll(Utils.getPropertiesFromFile(sparkConf.get("spark.properties.file")))
    }

    init(sparkConf)

    // 时间间隔
    val slide = sparkConf.get("spark.slide").toInt
    val sc = new SparkContext(sparkConf)
    val ssc = new StreamingContext(sc, Seconds(slide))

    ssc.addStreamingListener(new CongestionMonitorListener(ssc))
    if (monitor) ssc.addStreamingListener(new InkeStatsReportListener(ssc))

    handle(ssc)
    ssc
  }


  private def printUsageAndExit() = {

    System.err.println(
      """
        |"Usage: InkeStreaming [options]
        |
        | Options are:
        |   --monitor <是否开启监控  true|false>
        |   --checkpointPath <checkpoint 目录设置>
        |   --createOnError <从 checkpoint 恢复失败,是否重新创建 true|false>
        |""".stripMargin)
    System.exit(1)
  }


  def main(args: Array[String]): Unit = {

    this._args = args

    var argv = args.toList

    while (argv.nonEmpty) {
      argv match {
        case ("--monitor") :: value :: tail =>
          monitor = value.toBoolean
          argv = tail
        case ("--checkpointPath") :: value :: tail =>
          checkpointPath = value
          argv = tail
        case ("--createOnError") :: value :: tail =>
          createOnError = value.toBoolean
          argv = tail
        case Nil =>
        case tail =>
          System.err.println(s"Unrecognized options: ${tail.mkString(" ")}")
          printUsageAndExit()
      }
    }


    val context = checkpointPath match {
      case "" => creatingContext()
      case ck =>
        val ssc = StreamingContext.getOrCreate(ck, creatingContext, createOnError = createOnError)
        ssc.checkpoint(ck)
        ssc
    }

    context.start()
    context.awaitTermination()
  }
}
