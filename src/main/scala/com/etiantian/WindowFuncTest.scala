package com.etiantian

import java.util
import java.util.Properties

import com.etiantian.bigdata.JsonDeserializationSchema
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time

object WindowFuncTest {
  def main(args: Array[String]): Unit = {
    val senv = StreamExecutionEnvironment.getExecutionEnvironment

    val topicList = new util.ArrayList[String]()
    topicList.add("ycf1")

    val prop = new Properties()
    prop.setProperty("bootstrap.servers", "192.168.10.45:9092")
    prop.setProperty("group.id", "k1")
    prop.setProperty("auto.offset.reset", "latest")

    val kafka010Consumer = new FlinkKafkaConsumer010[String](topicList, new JsonDeserializationSchema, prop)

    val stream = senv.addSource(kafka010Consumer).windowAll(
      SlidingProcessingTimeWindows.of(Time.seconds(10),Time.seconds(5))
    )

    stream.reduce((a, b) => "Reduce =====> " + a + b).print()
    senv.execute()
  }
}
