package com.wplay.spark.streaming.core.kit

import java.util.Properties

import org.apache.spark.SparkConf
import org.apache.spark.sql.SaveMode

/**
  *
  * Created by james on 16/7/1.
  */
object MysqlKit {

  /**
    * 获取 Mysql 配置
    *
    * @param sparkConf
    * @return
    */
  def initParams(@transient sparkConf: SparkConf) = {
    val url = sparkConf.get("spark.mysql.url", "jdbc:mysql://localhost/test")
    val table = sparkConf.get("spark.mysql.output_table_name", "test")
    val saveMode = sparkConf.get("spark.mysql.saveMode", "append").toLowerCase() match {
      case "overwrite" => SaveMode.Overwrite
      case "errorifexists" => SaveMode.ErrorIfExists
      case "ignore" => SaveMode.Ignore
      case _ => SaveMode.Append
    }
    val connectionProperties = createMysqlProp(sparkConf)

    (saveMode, url, table, connectionProperties)
  }

  /**
    * 创建 mysql 配置
    *
    * @param sparkConf
    * @return
    */
  def createMysqlProp(@transient sparkConf: SparkConf): Properties = {
    val prop = new Properties()
    prop.put("driver", sparkConf.get("spark.mysql.driver", "com.mysql.jdbc.Driver"))
    prop.put("dbtable", sparkConf.get("spark.mysql.dbtable", "test"))
    prop.put("user", sparkConf.get("spark.mysql.user", "root"))
    prop.put("password", sparkConf.get("spark.mysql.password", ""))
    prop
  }
}
