//package com.wplay.spark.sql.core.acc
//
//import scala.collection.mutable
//
///**
//  * 自定义累加器
//  */
//class SessionAggrStatAccumulator extends Accumulator2[String, mutable.HashMap[String, Int]] {
//
//  // 保存所有聚合数据
//  private val aggrStatMap = mutable.HashMap[String, Int]()
//
//  override def isZero: Boolean = {
//    aggrStatMap.isEmpty
//  }
//
//  override def copy(): Accumulator2[String, mutable.HashMap[String, Int]] = {
//    val newAcc = new SessionAggrStatAccumulator
//    aggrStatMap.synchronized{
//      newAcc.aggrStatMap ++= this.aggrStatMap
//    }
//    newAcc
//  }
//
//  override def reset(): Unit = {
//    aggrStatMap.clear()
//  }
//
//  override def add(v: String): Unit = {
//    if (!aggrStatMap.contains(v))
//      aggrStatMap += (v -> 0)
//    aggrStatMap.update(v, aggrStatMap(v) + 1)
//  }
//
//  override def merge(other: Accumulator2[String, mutable.HashMap[String, Int]]): Unit = {
//    other match {
//      case acc:SessionAggrStatAccumulator => {
//        (this.aggrStatMap /: acc.value){ case (map, (k,v)) => map += ( k -> (v + map.getOrElse(k, 0)) )}
//      }
//    }
//  }
//
//  override def value: mutable.HashMap[String, Int] = {
//    this.aggrStatMap
//  }
//}
