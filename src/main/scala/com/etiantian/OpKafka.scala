package com.etiantian

import java.util.Properties

import com.etiantian.utils.HBaseOutputFormat
import org.apache.flink.api.common.typeinfo.BasicTypeInfo
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010
import org.apache.flink.streaming.util.serialization.{KeyedDeserializationSchema, SimpleStringSchema, TypeInformationKeyValueSerializationSchema}
import org.apache.flink.streaming.api.scala._
import org.apache.hadoop.hbase.client.Put
import org.json.JSONObject

object OpKafka {
  def main(args: Array[String]): Unit = {
    val senv = StreamExecutionEnvironment.getExecutionEnvironment
    senv.enableCheckpointing(500)

    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "192.168.10.45:9092")
    properties.setProperty("group.id", "t1")
    properties.setProperty("auto.offset.reset", "latest")
//    properties.setProperty("enable.auto.commit", "true")
//    properties.setProperty("auto.commit.interval.ms", "1000")

    val consumer010 = new FlinkKafkaConsumer010[String](
      "ycf1",
      new KeyedDeserializationSchema[String]() {
        override def isEndOfStream(nextElement: String) = false

        override def deserialize(messageKey: Array[Byte], message: Array[Byte], topic: String, partition: Int, offset: Long) = {
          val json = new JSONObject()
          json.put("topic", topic)
          json.put("partition", partition)
          json.put("offset", offset)
          json.put("key", if (messageKey == null) null else new String(messageKey))
          json.put("value", if (message == null) null else new String(message))
          json.toString()
        }

        override def getProducedType = BasicTypeInfo.STRING_TYPE_INFO
      },
      properties
    ).setStartFromGroupOffsets()

    val dataStream = senv.addSource(consumer010)
//      .print()
    dataStream.map(x => println(s"=====$x====="))
    dataStream.map(x => println(s"####$x####"))


    val parameter = new Configuration()
        parameter.setString("quorum", "t193,t194,t195")
//    parameter.setString("quorum", "cdh132,cdh133,cdh134")
    parameter.setString("port", "2181")
    parameter.setString("tableName", "test_kafka_city_info")
    val hBaseOutputFormat = new HBaseOutputFormat
    hBaseOutputFormat.setConfiguration(parameter)
    senv.addSource(consumer010).map(x => {
      val array = x.split(",")

      val put = new Put(array(0).getBytes())
      put.addColumn("position".getBytes(), "city_name".getBytes(), array(0).getBytes())
      put.addColumn("position".getBytes(), "lng".getBytes(), array(1).getBytes())
      put.addColumn("position".getBytes(), "lat".getBytes(), array(2).getBytes())
      put
    }).writeUsingOutputFormat(hBaseOutputFormat)
    senv.execute()
  }
}
