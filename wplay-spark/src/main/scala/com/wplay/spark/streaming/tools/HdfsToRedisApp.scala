package com.wplay.spark.streaming.tools

import com.wplay.spark.redis.{RedisConnectionPool, RedisEndpoint}
import notice.{Ding, send}
import org.apache.spark.{Logging, SparkConf, SparkContext}

/**
  * Created by james on 2017/7/24.
  */
object HdfsToRedisApp extends Logging {

  /*主机名*/
  private var hostname: String = "localhost"
  /*端口*/
  private var port: Int = 6379
  /*密码*/
  private var passward: String = ""
  /*目录*/
  private var path: String = ""
  /*前缀*/
  private var prefix: String = ""
  /*过期时间*/
  private var ttl: Int = Integer.MAX_VALUE

  // 钉钉发送接口
  private lazy val sendApi = "https://oapi.dingtalk.com/robot/send?access_token=36a782580d084741785a6c99ce2a8f20064f85eb8de6d35a290214984f5fe3db"


  def main(args: Array[String]): Unit = {

    var argv = args.toList

    while (argv.nonEmpty) {
      argv match {
        case ("-h") :: value :: tail =>
          hostname = value
          argv = tail
        case ("-p") :: value :: tail =>
          port = value.toInt
          argv = tail
        case ("-a") :: value :: tail =>
          passward = value
          argv = tail
        case ("-path") :: value :: tail =>
          path = value
          argv = tail
        case ("-prefix") :: value :: tail =>
          prefix = value
          argv = tail
        case ("-ttl") :: value :: tail =>
          ttl = value.toInt
          argv = tail
        case Nil =>
        case tail =>
          System.err.println(s"Unrecognized options: ${tail.mkString(" ")}")
          printUsageAndExit()
      }
    }

    handle()

  }


  private def handle(): Unit = {

    val start = System.currentTimeMillis()

    logInfo("Begin load data to redis...")
    val msg =
      s"""
         |
         |hostname:$hostname
         |port:$port
         |passward:$passward
         |path:$path
         |prefix:$prefix
         |ttl:$ttl
       """.stripMargin
    logInfo(msg)

    val sparkConf = new SparkConf().setMaster("local[3]").setAppName("test")

    val sc = new SparkContext(sparkConf)

    val logs = sc.textFile(path)

    val finalRdd = logs.map(_.split(",")).map(x => {
      s"$prefix${x.head}" -> x.last
    })//.cache()

    val endpoint = passward match {
      case "" => RedisEndpoint(hostname, port)
      case pwd => RedisEndpoint(hostname, port, pwd)
    }

    finalRdd.foreachPartition(iter => {
      val jedis = RedisConnectionPool.connect(endpoint)
      iter.foreach(x => jedis.setex(x._1, ttl, x._2))
      jedis.close()
    })


    val count = finalRdd.count()

    "".contains()
    val useTime = (System.currentTimeMillis() - start) / 1000

    val message = s"Done!!! write data size $count to redis, use time $useTime qps ${count * 1.0 / useTime} $msg"
    logInfo(message)
    send a Ding(sendApi, "", message)

    sc.stop()

  }


  private def printUsageAndExit() = {

    System.err.println(
      """
        |"Usage: InkeStreaming [options]
        |
        | Options are:
        |   -h <Redis hostname>
        |   -p <Redis port>
        |   -a <Redis password>
        |   -path <HDFS 目录>
        |   -prefix <写入Redis 前缀>
        |""".stripMargin)
    System.exit(1)
  }

}
