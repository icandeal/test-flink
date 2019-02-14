package com.etiantian

import org.apache.flink.addons.hbase.TableInputFormat
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.hadoop.hbase.{HBaseConfiguration, HConstants, TableName}
import org.apache.hadoop.hbase.client.{HTable, Put, Result, Scan}
import org.apache.flink.api.java.tuple._
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration

object OpHbase {
  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment

    println("====================================================================")
    println("=============================  read hbase  =========================")

//    val readConfiguration = new Configuration()
//    readConfiguration.setString("quorum", "cdh132,cdh133,cdh134")
//    readConfiguration.setString("port", "2181")
//    readConfiguration.setString("tableName", "beehive_user_dir_score")
//    readConfiguration.setString("family", "beehive_user_dir_score")
//
//    val tableInputFormat = new TableInputFormat[Tuple4[String, String, Double, Double]] {
//      var tableName: String = null
//      var family: String = null
//
//      override def mapResultToTuple(result: Result) = {
//        val jid = new String(result.getValue(family.getBytes(), "jid".getBytes()))
//        val dirId = new String(result.getValue(family.getBytes(), "dir_id".getBytes()))
//        val score = new String(result.getValue(family.getBytes(), "score".getBytes())).toDouble
//        val seriesScore = new String(result.getValue(family.getBytes(), "series_score".getBytes())).toDouble
//
//
//        new Tuple4(jid, dirId, score, seriesScore)
//      }
//
//      override def getTableName = tableName
//
//      override def getScanner = {
//        val scan = new Scan()
//        scan.addFamily(family.getBytes())
//        scan
//      }
//
//      override def configure(parameters: Configuration): Unit = {
//        val conf = HBaseConfiguration.create()
//        conf.set(HConstants.ZOOKEEPER_QUORUM, parameters.getString("quorum", null))
//        conf.set(HConstants.ZOOKEEPER_CLIENT_PORT, parameters.getString("port", "2181"))
//        tableName = parameters.getString("tableName", null)
//        family = parameters.getString("family", null)
//        table = new HTable(conf, tableName)
//        scan = getScanner
//      }
//    }
//
//    env.createInput(tableInputFormat).withParameters(readConfiguration).first(10).print()


    println("===================================================  new war  =====================================================")
//
    val readConfiguration = new Configuration()
//    readConfiguration.setString("quorum", "t193,t194,t195")
    readConfiguration.setString("quorum", "cdh132,cdh133,cdh134")
    readConfiguration.setString("port", "2181")
    readConfiguration.setString("tableName", "beehive_user_dir_score")
    readConfiguration.setString("family", "beehive_user_dir_score")
    readConfiguration.setString("columns", "jid,dir_id,score,series_score")
    env.createInput(
      new HBaseInputFormat[Tuple4[Long, Long, Double, Double]]
    ).withParameters(readConfiguration).first(10).print()

    println("====================================================================")
    println("============================  write hbase  =========================")

    val text = env.readTextFile("hdfs://cdh131:8020/data/city.csv")
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
