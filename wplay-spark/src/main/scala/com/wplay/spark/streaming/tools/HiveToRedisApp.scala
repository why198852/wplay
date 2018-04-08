package com.wplay.spark.streaming.tools

import com.wplay.spark.plugins.redis.{RedisConnectionPool, RedisEndpoint}
import com.wplay.spark.streaming.core.SQLContextSingleton
import notice.Ding
import org.apache.spark.{Logging, SparkConf, SparkContext}

/**
  * Created by james on 2017/7/24.
  */
object HiveToRedisApp extends Logging {

  /*主机名*/
  private var hostname: String = "localhost"
  /*端口*/
  private var port: Int = 6379
  /*密码*/
  private var passward: String = _
  /*目录*/
  private var hql: String = ""

  /*过期时间*/
  private var ttl: Int = Integer.MAX_VALUE

  private var repartition: Int = 0

  // 钉钉发送接口
  private lazy val sendApi = "https://oapi.dingtalk.com/robot/send?access_token=36a782580d084741785a6c99ce2a8f20064f85eb8de6d35a290214984f5fe3db"


  def main(args: Array[String]): Unit = {

    /*前缀*/
    var prefix: String = ""
    var batch: Int = 5000

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
        case ("--hql") :: value :: tail =>
          hql = value
          argv = tail
        case ("--prefix") :: value :: tail =>
          prefix = value
          argv = tail
        case ("--ttl") :: value :: tail =>
          ttl = value.toInt
          argv = tail
        case ("--repartition") :: value :: tail =>
          repartition = value.toInt
          argv = tail
        case ("--batch") :: value :: tail =>
          batch = value.toInt
          argv = tail
        case Nil =>
        case tail =>
          System.err.println(s"Unrecognized options: ${tail.mkString(" ")}")
          printUsageAndExit()
      }
    }

    handle()

    def handle(): Unit = {


      val msg =
        s"""
           |Begin load data to redis...
           |
           |hostname:$hostname
           |port:$port
           |passward:$passward
           |hql:$hql
           |prefix:$prefix
           |ttl:$ttl
           |batch:$batch
       """.stripMargin
      logInfo(msg)
      import notice._
      send a Ding(sendApi, "", msg)


      val endpoints = hostname.split(",")
        .map(host => RedisEndpoint(host = host, port = port, auth = passward, timeout = 1000 * 60 * 10)).toList.toArray


      val sparkConf = new SparkConf()

      val sc = new SparkContext(sparkConf)

      val hiveContext = SQLContextSingleton.getHiveContext(sc)

      val logsDF = if (repartition > 0) hiveContext.sql(hql).repartition(repartition) else hiveContext.sql(hql)

      val finalRdd = logsDF.map(x => {
        s"$prefix${x.get(0)}" -> s"${x.get(1)}"
      }).cache()

      val count = finalRdd.count()

      val start = System.currentTimeMillis()

      finalRdd.foreachPartition(iter => {

        val jedis = RedisConnectionPool.connect(endpoints)

        val pipe = jedis.pipelined()

        var count = 0

        iter.foreach(x => {
          count = count + 1
          pipe.setex(x._1, ttl, x._2)
          if (count % batch == 0) pipe.sync()
        })

        pipe.sync()
        jedis.close()

        //        println(s"one partition write data [$count] user time [${System.currentTimeMillis() - start}]")
      })

      val useTime = (System.currentTimeMillis() - start) / 1000


      val message = s"Done!!! write data size $count to redis, use time $useTime s, qps ${count * 1.0 / useTime} $msg"
      logInfo(message)
      send a Ding(sendApi, "", message)

      sc.stop()

    }
  }


  private def printUsageAndExit() = {

    System.err.println(
      """
        |"Usage: InkeStreaming [options]
        |
        | Options are:
        |     -h              <Redis hostname>
        |     -p              <Redis port>
        |     -a              <Redis password>
        |   --hql             <Hive sql>
        |   --prefix          <写入Redis 前缀>
        |   --ttl             <超时时间>
        |   --repartition     <分区>
        |""".stripMargin)
    System.exit(1)
  }

}
