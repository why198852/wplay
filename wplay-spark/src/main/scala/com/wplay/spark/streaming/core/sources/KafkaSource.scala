package com.wplay.spark.streaming.core.sources

import java.util.concurrent.atomic.AtomicReference

import com.wplay.core.util.StringKit
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaManager, OffsetRange}

/**
  * Created by james on 16/8/2.
  *
  * 读取Kafka数据
  *
  * @param ssc        StreamingContext
  * @param topics     Topics
  * @param initParams 初始化 Kafka 参数
  */
class KafkaSource(@transient val ssc: StreamingContext,
                  topics: String = null,
                  initParams: Map[String, String] = Map.empty[String, String])
  extends Source[(String, String)] {


  // 保存 offset
  private lazy val offsetRanges = new AtomicReference[Array[OffsetRange]]

  // 分区数
  private lazy val repartition: Int = sparkConf.get("spark.partition", "0").toInt

  // kafka 消费 topic
  private lazy val topicSet: Set[String] = {
    if (StringKit.nonEmpty(topics))
      topics.split(",").map(_.trim).toSet
    else
      sparkConf.get("spark.source.kafka.consume.topics").split(",").map(_.trim).toSet
  }

  // kafka 代理
  private lazy val brokers = sparkConf
    .getOption("spark.source.kafka.metadata.broker.list")
    .getOrElse(initParams("metadata.broker.list"))

  // kafka 消费分组
  private lazy val groupId = sparkConf
    .getOption("spark.source.kafka.consume.group.id")
    .orElse(initParams.get("group.id"))

  // 首次消费 offset 的位置
  private lazy val reset: String = sparkConf
    .get("spark.source.kafka.auto.offset.reset", "largest")

  // 连接 Kafka 网络超时时间
  private lazy val socket_timeout_ms: String = sparkConf
    .get("spark.source.kafka.consume.socket.timeout.ms", "30000")

  private lazy val brokersSize: Int = brokers.split(",").length

  // kafka 参数
  private lazy val kafkaParams: Map[String, String] = {
    Map[String, String](
      "metadata.broker.list" -> brokers,
      "socket.timeout.ms" -> socket_timeout_ms,
      "auto.offset.reset" -> reset
    ) ++ {
      groupId match {
        case Some(gid) => Map[String, String]("group.id" -> gid)
        case None => Map.empty[String, String]
      }
    } ++ initParams
  }

  val km = new KafkaManager(kafkaParams)

  /**
    * 获取DStream 流
    *
    * @return
    */
  override def getDStream: DStream[(String, String)] = getDStream(mmd => (mmd.key, mmd.message))

  /**
    * 通过指定messageHandler 获得Kafka 数据流
    *
    * @param mmd
    * @return
    */
  def getDStream(mmd: MessageAndMetadata[String, String] => (String, String)
                 =
                 mmd => (mmd.key, mmd.message)): DStream[(String, String)] = {

    km.createDirectStream[
      String, String,
      StringDecoder,
      StringDecoder, (String, String)](ssc, kafkaParams, topicSet, mmd)
      .transform(rdd => {
        offsetRanges.set(rdd.asInstanceOf[HasOffsetRanges].offsetRanges)
        if (repartition > 0) rdd.repartition(repartition) else rdd
      })
  }

  /**
    * 更新Offset 操作 一定要放在所有逻辑代码的最后
    * 这样才能保证,只有action执行成功后才更新offset
    */
  def updateZKOffsets(): Unit = {
    // 更新 offset
    if (groupId.isDefined)
      km.updateZKOffsets(groupId.get, offsetRanges.get())
  }

}
