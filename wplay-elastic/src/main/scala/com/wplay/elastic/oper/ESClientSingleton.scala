package com.wplay.elastic.oper

import java.net.InetAddress

import com.wplay.core.util.{ConfigUtil, XmlConfigUtil}
import org.elasticsearch.client.Client
import org.elasticsearch.client.transport.TransportClient
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.transport.InetSocketTransportAddress
import org.elasticsearch.transport.client.PreBuiltTransportClient

/**
  * Created by wangy3 on 2016/12/29.
  *
  * ES ¿Í»§¶Ë
  */
object ESClientSingleton {
  @transient private var instance: Client = _
  @transient val conf = ConfigUtil.getConfiguration("elastic.properties")
  @transient val hosts = conf.getString("elastic.hosts")
  @transient val port = conf.getInt("elastic.port")

  def getInstance(): Client = {
    if (instance == null) {
      instance = getTransportClient
    }
    instance
  }

  private def getTransportClient: TransportClient = {
    val settings: Settings = Settings.builder()
      .put("cluster.name", "es")
      .put("transport.type","netty3")
      .put("http.type", "netty3")
      .build

    val tc = new PreBuiltTransportClient(settings)
    for(hostname <- hosts.split(",")) tc.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(hostname), port))

//      .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("hadoop97"), 9300))
//      .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("hadoop98"), 9300))
//      .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("hadoop99"), 9300))
//      .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("hadoop100"), 9300))
//      .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("hadoop101"), 9300))
    tc
  }


  def main(args: Array[String]): Unit = {

    val client = ESClientSingleton.getInstance()

    val resp = client.admin().indices().prepareTypesExists("newserverlog_origside_ikut_online").setTypes("ymd_201803401").execute().get()

    print(resp)
  }
}
