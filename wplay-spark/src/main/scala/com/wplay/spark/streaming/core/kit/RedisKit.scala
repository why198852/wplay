package com.wplay.spark.streaming.core.kit

import redis.clients.jedis.Jedis

/**
  *
  * Created by james on 16/7/1.
  *
  * Redis 工具包
  */
object RedisKit {

  @volatile private var client: Client = null

  /**
    * 获取单例客户端
    *
    * @param host 主机
    * @param port 端口
    * @param db   库 默认为 0
    * @return
    */
  def getClient(host: String, port: Int, db: Int = 0): Client = {
    if (client == null) {
      synchronized {
        if (client == null) client = new Client(host, port, db)
      }
    }
    client
  }

  class Client(host: String, port: Int, db: Int) extends Serializable {

    @volatile private lazy val jedis = new Jedis(host, port, db)
    @volatile private lazy val pipelined = jedis.pipelined()

    /**
      * KV 数据写入 Redis
      *
      * @param items
      */
    def setex(items: Iterator[(String, Int, String)]): Unit = {
      var count = 0
      items.foreach((t: (String, Int, String)) => {
        count += 1
        pipelined.setex(t._1, t._2, t._3)
        if (count % 10000 == 0) {
          pipelined.sync()
          Thread.sleep(1)
        }
      })
      pipelined.sync()
    }

    /**
      * 监测Key是否存在
      *
      * @param key
      * @return
      */
    def exists(key: String): Boolean = {
      jedis.exists(key)
    }
  }

  def main(args: Array[String]): Unit = {


  }

}
