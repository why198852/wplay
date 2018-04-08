import com.wplay.spark.redis.{RedisConnectionPool, RedisEndpoint}
import com.wplay.spark.redis.RedisConnectionPool._

/**
  * Created by james on 18/4/8.
  */
object Main {
  def main (args: Array[String]): Unit = {

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
