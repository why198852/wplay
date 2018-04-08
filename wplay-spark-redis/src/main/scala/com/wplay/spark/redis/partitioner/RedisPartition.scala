package com.wplay.spark.redis.partitioner

import com.wplay.spark.redis.RedisConfig
import org.apache.spark.Partition


case class RedisPartition(index: Int,
                          redisConfig: RedisConfig,
                          slots: (Int, Int)) extends Partition