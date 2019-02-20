package com.etiantian.utils

import org.apache.flink.addons.hbase.TableInputFormat
import org.apache.flink.api.java.tuple._
import org.apache.flink.configuration.Configuration
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.{CellUtil, HBaseConfiguration, HConstants, TableName}

import scala.reflect.ClassTag

class HBaseInputFormat[T <: Tuple: ClassTag] extends TableInputFormat[T] {

  var tableName: String = null
  var family: String = null
  var columns: String = null
  var columnMap = Map[String, String]()

  override def configure(parameters: Configuration): Unit = {
    val conf = HBaseConfiguration.create()
    conf.set(HConstants.ZOOKEEPER_QUORUM, parameters.getString("quorum", null))
    conf.set(HConstants.ZOOKEEPER_CLIENT_PORT, parameters.getString("port", "2181"))
    tableName = parameters.getString("tableName", null)
    family = parameters.getString("family", null)
    columns = parameters.getString("columns", null)
    table =  ConnectionFactory.createConnection(conf).getTable(TableName.valueOf(tableName)).asInstanceOf[HTable]
    scan = getScanner

  }

  override def mapResultToTuple(r: Result): T = {
    for (cell <- r.rawCells()) {
      val qualifier = new String(CellUtil.cloneQualifier(cell))
      val value = new String(CellUtil.cloneValue(cell))
      columnMap += (qualifier -> value)
    }

    val clazz = implicitly[ClassTag[T]].runtimeClass
    val instance = clazz.newInstance()
    val method = clazz.getMethods.filter(_.getName.equals("setField"))(0)

    val columnArray = columns.split(",")
    for (i <- 0 until columnArray.length) {
      val a = columnMap(columnArray(i))
      method.invoke(instance, a, new Integer(i))
    }
    instance.asInstanceOf[T]
  }


  override def getTableName = tableName

  override def getScanner = {
    val scan = new Scan()
    scan.addFamily(family.getBytes())
    columns.split(",").foreach(x => {
      scan.addColumn(family.getBytes(), x.getBytes())
    })
    scan
  }
}
