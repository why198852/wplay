package com.wplay.redis

import java.util.concurrent.ConcurrentHashMap

import com.typesafe.config.{Config, ConfigFactory}
import org.slf4j.LoggerFactory
import redis.clients.jedis.exceptions.JedisConnectionException
import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}

import scala.collection.JavaConversions._
import scala.util.{Failure, Success, Try}

object RedisConnectionPool {

  private lazy val logger = LoggerFactory.getLogger(getClass)

  @transient private lazy val pools: ConcurrentHashMap[RedisEndpoint, JedisPool] =
    new ConcurrentHashMap[RedisEndpoint, JedisPool]()


  def connect(configPath: String): Jedis = {
    connect(ConfigFactory.load(configPath))
  }

  def connect(): Jedis = {
    connect(ConfigFactory.load)

  }

  def connect(config: Config): Jedis = {
    val host = config.getString("redis.host")
    val port = Try(config.getString("redis.port").toInt).getOrElse(6379)
    val auth = Try(config.getString("redis.password")).getOrElse(null)

    connect(RedisEndpoint(host, port, auth))
  }

  /**
    * 随机选择一个 RedisEndpoint 创建 或者获取一个Redis 连接池
    *
    * @param res
    * @return
    */
  def connect(res: Array[RedisEndpoint]): Jedis = {
    assert(res.length > 0, "The RedisEndpoint array is empty!!!")
    val rnd = scala.util.Random.nextInt().abs % res.length
    try {
      connect(res(rnd))
    } catch {
      case e: Exception => e.printStackTrace()
        connect(res.drop(rnd))
    }
  }

  /**
    * 创建或者获取一个Redis 连接池
    *
    * @param re
    * @return
    */
  def connect(re: RedisEndpoint): Jedis = {
    val pool = pools.getOrElseUpdate(re, createPool(re))
    var sleepTime: Int = 4
    var conn: Jedis = null
    while (conn == null) {
      try {
        conn = pool.getResource
      } catch {
        case e: JedisConnectionException if e.getCause.toString.
          contains("ERR max number of clients reached") => {
          if (sleepTime < 500) sleepTime *= 2
          Thread.sleep(sleepTime)
        }
        case e: Exception => throw e
      }
    }
    conn
  }

  /**
    * 创建一个连接池
    *
    * @param re
    * @return
    */
  def createPool(re: RedisEndpoint): JedisPool = {

    println(s"createJedisPool with $re ")
    val poolConfig: JedisPoolConfig = new JedisPoolConfig()
    /*最大连接数*/
    poolConfig.setMaxTotal(1000)
    /*最大空闲连接数*/
    poolConfig.setMaxIdle(64)
    /*在获取连接的时候检查有效性, 默认false*/
    poolConfig.setTestOnBorrow(true)
    poolConfig.setTestOnReturn(false)
    /*在空闲时检查有效性, 默认false*/
    poolConfig.setTestWhileIdle(false)
    /*逐出连接的最小空闲时间 默认1800000毫秒(30分钟)*/
    poolConfig.setMinEvictableIdleTimeMillis(1800000)
    /*逐出扫描的时间间隔(毫秒) 如果为负数,则不运行逐出线程, 默认-1*/
    poolConfig.setTimeBetweenEvictionRunsMillis(30000)
    poolConfig.setNumTestsPerEvictionRun(-1)
    new JedisPool(poolConfig, re.host, re.port, re.timeout, re.auth, re.dbNum)

  }

  /**
    * Wrap Jedis close function
    * for example
    * {{{
    *    implicit val jedis = RedisConnectionPool.connect(RedisEndpoint())
    *
    *    val dbSize = safeClose {
    *         jedis => jedis.dbSize()
    *    }
    *
    *    println(s"dbSize $dbSize")
    *
    * }}}
    *
    * @param f
    * @param jedis
    * @tparam R
    * @return
    */
  def safeClose[R](f: Jedis => R)(implicit jedis: Jedis): R = {
    val result = f(jedis)
    Try {
      jedis.close()
    } match {
      case Success(v) => logger.debug(s"success close jedis $jedis")
      case Failure(e) => logger.error(s"failure close jedis ${e.getMessage}")
    }
    result
  }

  def main(args: Array[String]): Unit = {


    val dbSize = safeClose { jedis =>

      new Thread(new Runnable {
        override def run(): Unit = {
          Thread.sleep(10000)
          println(jedis.dbSize())
        }
      }).start()

    }(RedisConnectionPool.connect(RedisEndpoint()))


    println(s"dbSize $dbSize")


  }


}