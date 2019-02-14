package com.etiantian

import org.apache.flink.api.common.io.OutputFormat
import org.apache.flink.configuration.Configuration
import org.apache.hadoop.hbase.{HBaseConfiguration, HConstants, TableName}
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory, Put, Table}

class HBaseOutputFormat() extends OutputFormat[Put] {
  var table: Table = null
  var conf: org.apache.hadoop.conf.Configuration = null
  var tableName: String = null
  var conn: Connection = null

  override def configure(configuration: Configuration) = {
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
