package com.diwo.mdbase.dataprep.meta

import org.apache.spark.sql.DataFrame

import scala.collection.mutable.ListBuffer
import scala.io

object utilities {

  def time[R](block: => R): R = {
    val t0 = System.nanoTime()
    val result = block // call-by-name
    val t1 = System.nanoTime()
    println("Elapsed time: " + (t1 - t0) + "ns")
    result
  }

  def readFile(filename: String): Seq[String] = {
    val bufferedSource = io.Source.fromFile(filename)
    val lines = (for (line <- bufferedSource.getLines()) yield line).toList
    bufferedSource.close
    lines
  }

  def groupAndAggregate(df: DataFrame,  aggregateFun: Map[String, String], cols: List[String] ): DataFrame ={
    val grouped = df.groupBy(cols.head, cols.tail: _*)
    val aggregated = grouped.agg(aggregateFun)
    aggregated
  }


  def getColumns(cuboidID: String, cuboidDimMap: Map[Int,Map[Char,String]]):List[String] = {
    var ColList:ListBuffer[String] = ListBuffer.empty[String]
    val cID = cuboidID.split("_")(1)
    cuboidDimMap.keys.map{ key =>
      val cValue = cID.charAt(key)
      if(cValue != '*'){
        ColList += cuboidDimMap(key)(cValue)
      }
    }
    ColList.toList
  }
}
