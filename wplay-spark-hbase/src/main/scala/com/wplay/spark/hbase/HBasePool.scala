package com.wplay.spark.hbase

import java.util.concurrent.ConcurrentHashMap

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory}
import org.apache.spark.SparkConf

import scala.collection.JavaConversions._

/**
  * Created by james on 2017/10/18.
  *
  * HBase 连接池
  */
object HBasePool {


  @transient
  private lazy val pools: ConcurrentHashMap[Configuration, Connection] = new ConcurrentHashMap[Configuration, Connection]()


  def connect(params: Map[String, String]): Connection = {
    val conf = HBaseConfiguration.create
    for ((key, value) <- params) {
      conf.set(key, value)
    }
    connect(conf)
  }

  def connect(sparkConf: SparkConf): Connection = {

    val conf = HBaseConfiguration.create

    for ((key, value) <- sparkConf.getAll.filter(_._1.startsWith("spark.hbase")).map(x=>x._1->x._2).toMap) {
      conf.set(key.substring(6), value)
    }

    connect(conf)
  }

  def connect(conf: Configuration): Connection = {
    pools.getOrElseUpdate(conf, ConnectionFactory.createConnection(conf))
  }

}