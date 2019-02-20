package com.etiantian.utils

import org.apache.flink.api.common.io.OutputFormat
import org.apache.flink.configuration.Configuration
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory, Put, Table}
import org.apache.hadoop.hbase.{HBaseConfiguration, HConstants, TableName}

class HBaseOutputFormat() extends OutputFormat[Put] {
  var table: Table = null
  var conf: org.apache.hadoop.conf.Configuration = null
  var flinkConf: Configuration = new Configuration()
  var tableName: String = null
  var conn: Connection = null

  def setConfiguration(configuration: Configuration) = {
    flinkConf = configuration
  }

  def getConfig(field: String): Any = {
    if (flinkConf != null) flinkConf.getString(field, null) else null
  }

  override def configure(configuration: Configuration) = {
    configuration.addAll(flinkConf)
    println("=================================="+ configuration+"============================")
    conf = HBaseConfiguration.create()
    conf.set(HConstants.ZOOKEEPER_QUORUM, configuration.getString("quorum", null))
    conf.set(HConstants.ZOOKEEPER_CLIENT_PORT, configuration.getString("port", "2181"))

    tableName = configuration.getString("tableName", null)
  }

  override def writeRecord(it: Put) = {
    table.put(it)
  }

  override def close() = {
    if (table != null) table.close
    if (conn != null) conn.close
  }

  override def open(i: Int, i1: Int) = {
    conn = ConnectionFactory.createConnection(conf)
    table = conn.getTable(TableName.valueOf(tableName))
  }
}
