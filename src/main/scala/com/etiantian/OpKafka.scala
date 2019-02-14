package com.etiantian

import java.util.Properties

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010
import org.apache.flink.streaming.util.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._

object OpKafka {
  def main(args: Array[String]): Unit = {
    val senv = StreamExecutionEnvironment.getExecutionEnvironment
    senv.enableCheckpointing(500)

    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "192.168.10.45:9092")
    properties.setProperty("group.id", "t16")
    properties.setProperty("auto.offset.reset", "earliest")
//    properties.setProperty("enable.auto.commit", "true")
//    properties.setProperty("auto.commit.interval.ms", "1000")

    val consumer010 = new FlinkKafkaConsumer010[String](
      "ycf1",
      new SimpleStringSchema(),
      properties
    ).setStartFromGroupOffsets()

    senv.addSource(consumer010).map(x => println(s"=====$x=="))

    senv.execute()
  }
}
