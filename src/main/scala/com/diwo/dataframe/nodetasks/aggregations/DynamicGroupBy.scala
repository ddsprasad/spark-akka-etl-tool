package com.diwo.dataframe.nodetasks.aggregations

import com.diwo.dataframe.nodetasks.TasksRegistryHandler
import com.diwo.dataframe.nodetasks.sinks.CSVWriter
import com.diwo.mdbase.dataprep.meta.Messages.NodeTaskOptions
import com.diwo.mdbase.dataprep.module.ModuleApp
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

import com.diwo.mdbase.dataprep.meta.utilities._
import com.diwo.mdbase.dataprep.module.ModuleApp._

import scala.collection.mutable.ListBuffer

object DynamicGroupBy {

  val Dynamicgroupby = (optionsIn: NodeTaskOptions, dataframe: DataFrame) => {
    var t0 = System.nanoTime()
    ModuleApp.logger.info(s"TASK: ${DynamicGroupBy.getClass} STARTED ..........")
    var filepath = optionsIn.options.get("filepath").get.value.toString
    var filepath1 = "E:\\DataPrep-Spark\\src\\main\\resources\\GIO_renewals_IDs.csv"

    // var filepath1="C:\\hyderabad_team\\akka-applications\\src\\main\\resources\\L1.out"
    //var d = dataframe
    val sumexprs = dataframe.columns.map((_ -> "sum")).toMap

    //WCAKSK
    import scala.io.Source
    val cubiodList = Source.fromFile(filepath1).getLines.toList

//    val cubiodList = List("gioru_WC**SK","gioru_WCAKSK","gioru_WC**SK")
    val cuboidDimMap = Map(0 -> Map('W' -> "week_ending"), 1 -> Map('C' -> "Coverage"), 2 -> Map('A' -> "YoungestAge"), 3 -> Map('K' -> "AnnualKM"), 4 -> Map('S' -> "State"), 5 -> Map('K' -> "CrestaZone"))

    var finalDf: DataFrame = null
//    var df:DataFrame = null

    for (cuboid <- cubiodList) {

     val finalDf1 = groupAndAggregate(dataframe, sumexprs, getColumns(cuboid, cuboidDimMap)).withColumn("CuboidID",lit(cuboid))
      val cols = array(getColumns(cuboid, cuboidDimMap).map(c =>
        // If column is not null, merge it with its name otherwise null
        when(col(c).isNotNull, concat_ws("|", col(c)))): _*
      )

      val combine = udf((xs: Seq[String]) => {
        val tmp = xs.filter { _ != null }.mkString("|")
        s"{$tmp}"
      })

      finalDf = finalDf1.withColumn("RowKey",combine(cols))
       //.selectExpr("concat(nvl(week_ending, ''), nvl(CrestaZone, ''), nvl(Coverage, ''), nvl(State, '')) as RowKey")

    //  finalDf.createOrReplaceTempView("ruTable")

     // var df = spark.sql(s"select week_ending,CrestaZone,Coverage,YoungestAge,AnnualKM,sum(offered),sum(RetailPremOffered),sum(Avg_RN_BasePrem_Offered),sum(YoungestAge),sum(renewed),sum(RetailPremPaid),sum(Avg_RN_RetPrem_Offered),sum(CrestaZone),sum(Coverage),sum(Avg_RN_RetPrem_Paid),sum(Avg_RN_BasePrem_Paid),sum(Renewal_Rate),sum(AnnualKM),sum(BasePremExpiry),sum(RetailPremExpiry),sum(State),sum(BasePremWritten),sum(BasePremOffered),sum(week_ending), ${cuboid.toString} as CubiodID")

      var t1 = System.nanoTime()
      var result = t1 - t0
      ModuleApp.logger.info(s"TASK: ${DynamicGroupBy.getClass} SUCCESFULLY DONE IN JUST $result")
      var finalResult = t1 - ModuleApp.tstart
      ModuleApp.logger.info(s"TOTAL TAKEN :   $finalResult")
      println("PROCESS SUCCESFULLY COMPLETED")
      //TODO as the Caller requries Optional(dataframe)
//      Option(df)
     // Option(finalDf)

      finalDf.coalesce(1).write.mode("append").format("com.databricks.spark.csv").option("header",true).save(filepath)
    }
//    Option(df)
    Option(finalDf)
  }

  TasksRegistryHandler.tasksRegistry.put("dynamicgroupby", Dynamicgroupby)

}