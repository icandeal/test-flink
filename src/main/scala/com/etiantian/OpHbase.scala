package com.etiantian

import com.etiantian.utils.{HBaseInputFormat, HBaseOutputFormat}
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.hadoop.hbase.client.{Put, Scan}
import org.apache.flink.api.java.tuple._
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration

object OpHbase {
  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment

    println("====================================================================")
    println("=============================  read hbase  =========================")
    val readConfiguration = new Configuration()
    readConfiguration.setString("quorum", "t193,t194,t195")
//    readConfiguration.setString("quorum", "cdh132,cdh133,cdh134")
    readConfiguration.setString("port", "2181")
    readConfiguration.setString("tableName", "beehive_user_dir_score")
    readConfiguration.setString("family", "beehive_user_dir_score")
    readConfiguration.setString("columns", "jid,dir_id,score,series_score")
    env.createInput(
      new HBaseInputFormat[Tuple4[String, String, String, String]]()
    ).withParameters(readConfiguration).first(10).print()

    println("====================================================================")
    println("============================  write hbase  =========================")
//
    val text = env.readTextFile("hdfs://nameservice1/data/city.csv")
    val parameter = new Configuration()
//    parameter.setString("quorum", "t193,t194,t195")
    parameter.setString("quorum", "cdh132,cdh133,cdh134")
    parameter.setString("port", "2181")
    parameter.setString("tableName", "city_position_info")
    text.first(10).print()
    text.map(x => {
      val array = x.split(",")

      val put = new Put(array(0).getBytes())
      put.addColumn("position".getBytes(), "city_name".getBytes(), array(0).getBytes())
      put.addColumn("position".getBytes(), "lng".getBytes(), array(1).getBytes())
      put.addColumn("position".getBytes(), "lat".getBytes(), array(2).getBytes())
    }).output(new HBaseOutputFormat).withParameters(parameter)

    env.execute()
  }
}
