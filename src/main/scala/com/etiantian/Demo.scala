package com.etiantian

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala._
import org.apache.flink.api.scala.extensions._
/**
 * Hello world!
 *
 */
object Demo {
  def main(args: Array[String]) : Unit = {

    val path = ParameterTool.fromArgs(args).get("path")
    // get the execution environment
    val env = ExecutionEnvironment.getExecutionEnvironment

    val text = env.readTextFile(path)

    // parse the data, group it, window it, and aggregate the counts
    val windowCounts = text
      .flatMap { w => w.split("\\s") }
      .map { (_, 1) }
        .groupBy(0)
      .sum(1)

    // print the results with a single thread, rather than in parallel
    windowCounts.print()

  }
}
