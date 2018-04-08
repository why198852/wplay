package com.wplay.spark.streaming.core

import com.typesafe.config.{Config, ConfigFactory}


/**
  * Created by james on 16/8/24.
  *
  * 默认配置
  */
trait WplayConfig {
  val config: Config = ConfigFactory.load()
}
