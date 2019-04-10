package com.etiantian


import java.net.{InetAddress, InetSocketAddress}
import java.util.Properties

import com.etiantian.bigdata.JsonDeserializationSchema
import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.elasticsearch.{ElasticsearchSinkFunction, RequestIndexer}
import org.apache.flink.streaming.connectors.elasticsearch5.ElasticsearchSink
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.common.xcontent.json.JsonXContent
import org.apache.flink.api.scala._
import org.json.JSONObject

import scala.collection.JavaConversions._

object OpEs {
  def main(args: Array[String]): Unit = {

    val config = new java.util.HashMap[String, String]
    config.put("cluster.name", "espro")
    config.put("bulk.flush.max.actions", "1")

    val addressList = List(
      new InetSocketAddress(InetAddress.getByName("10.1.5.221"), 9300),
      new InetSocketAddress(InetAddress.getByName("10.1.5.222"), 9300),
      new InetSocketAddress(InetAddress.getByName("10.1.5.223"), 9300)
    )

//    val addressList = new java.util.ArrayList[InetSocketAddress]
//    addressList.add(new InetSocketAddress(InetAddress.getByName("gw1in.aliyun.etiantian.net"), 12213))
    val senv = StreamExecutionEnvironment.getExecutionEnvironment
    senv.enableCheckpointing(500)

    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "cdh132:9092,cdh133:9092,cdh134:9092")
    properties.setProperty("group.id", "p2")
    properties.setProperty("auto.offset.reset", "latest")


    val consumer010 = new FlinkKafkaConsumer010[String](
      "ubuntu.school.user_info",
      new JsonDeserializationSchema(),
      properties
    ).setStartFromGroupOffsets()

//    val addressList = List(
//      new InetSocketAddress(InetAddress.getByName("gw1in.aliyun.etiantian.net"), 12213)
//    )
//    senv.addSource[String](consumer010).print()
    senv.addSource[String](consumer010).addSink(new ElasticsearchSink[String](config, addressList, new ElasticsearchSinkFunction[String]{
      override def process(t: String, runtimeContext: RuntimeContext, requestIndexer: RequestIndexer) = {
        try {
          val json = new JSONObject(new JSONObject(t).get("value").toString)

          val content = JsonXContent.contentBuilder().startObject()
            .field("before", json.get("before").toString)
            .field("after", json.get("after").toString)
            .endObject()

          val indexRequest = new IndexRequest().index(
            "ubuntu"
          ).`type`(
            "data"
          ).id(json.get("ts_ms").toString).source(content)
          requestIndexer.add(indexRequest)
        } catch {
          case e:Exception => println("++++++++++++++++++++++++++++++++++++++++"+t)
        }
      }
    }))

    senv.execute()
  }
}
