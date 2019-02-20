package com.etiantian


import java.net.{InetAddress, InetSocketAddress}
import java.util.Properties

import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.elasticsearch.{ElasticsearchSinkFunction, RequestIndexer}
import org.apache.flink.streaming.connectors.elasticsearch5.ElasticsearchSink
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010
import org.apache.flink.streaming.util.serialization.SimpleStringSchema
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.common.xcontent.json.JsonXContent
import org.apache.flink.api.scala._
import scala.collection.JavaConversions._

object OpEs {
  def main(args: Array[String]): Unit = {

    val config = new java.util.HashMap[String, String]
    config.put("cluster.name", "espro")
    config.put("bulk.flush.max.actions", "1")

//    val addressList = List(
//      new InetSocketAddress(InetAddress.getByName("10.1.5.221"), 9300),
//      new InetSocketAddress(InetAddress.getByName("10.1.5.222"), 9300),
//      new InetSocketAddress(InetAddress.getByName("10.1.5.223"), 9300)
//    )

//    val addressList = new java.util.ArrayList[InetSocketAddress]
//    addressList.add(new InetSocketAddress(InetAddress.getByName("gw1in.aliyun.etiantian.net"), 12213))
    val senv = StreamExecutionEnvironment.getExecutionEnvironment
    senv.enableCheckpointing(500)

    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "192.168.10.45:9092")
    properties.setProperty("group.id", "t2")
    properties.setProperty("auto.offset.reset", "earliest")


    val consumer010 = new FlinkKafkaConsumer010[String](
      "ycf1",
      new SimpleStringSchema(),
      properties
    ).setStartFromGroupOffsets()

    val addressList = List(
      new InetSocketAddress(InetAddress.getByName("gw1in.aliyun.etiantian.net"), 12213)
    )

    senv.addSource[String](consumer010).addSink(new ElasticsearchSink[String](config, addressList, new ElasticsearchSinkFunction[String]{
      override def process(t: String, runtimeContext: RuntimeContext, requestIndexer: RequestIndexer) = {
        if (t != null && t.count(_ == ',')==2) {
          try {
            val array = t.split(",")

            val content = JsonXContent.contentBuilder().startObject()
              .field("city_name", array(0))
              .field("lat", array(1))
              .field("lng", array(2))
              .endObject()

            val indexRequest = new IndexRequest().index(
              "city_info_flink"
            ).`type`(
              "city"
            ).id(array(0)).source(content)
            requestIndexer.add(indexRequest)
          } catch {
            case e:Exception => e.printStackTrace()
          }
        }
      }
    }))

    senv.execute()
  }
}
