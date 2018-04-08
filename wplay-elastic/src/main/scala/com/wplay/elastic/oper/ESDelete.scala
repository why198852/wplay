package com.wplay.elastic.oper

/**
  * Created by wangy3 on 2017/3/30.
  */
object ESDelete {
  private val client = ESClientSingleton.getInstance()
  def  deleteIndex(indexName:String): Unit ={
    client.admin().indices().prepareDelete(indexName).execute().actionGet();
  }
  def deleteIndexByType(indexName:String,indexType:String): Unit ={
    client.admin().indices().prepareDelete(indexName,indexType).execute().actionGet()
  }

  def  main(args:Array[String]){
    deleteIndex("page")
  }

  def createIndexStructure(): Unit ={

  }
}
