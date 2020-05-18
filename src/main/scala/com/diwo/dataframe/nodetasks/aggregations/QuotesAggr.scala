package com.diwo.dataframe.nodetasks.aggregations

import com.diwo.dataframe.nodetasks.TasksRegistryHandler
import com.diwo.dataframe.nodetasks.sinks.CSVWriter
import com.diwo.mdbase.dataprep.meta.Messages.NodeTaskOptions
import com.diwo.mdbase.dataprep.module.ModuleApp
import org.apache.spark.sql.DataFrame

object QuotesAggr {

  val Quotesaggr = (optionsIn: NodeTaskOptions, dataframe: DataFrame) => {
    var t0 = System.nanoTime()
    ModuleApp.logger.info(s"TASK: ${CSVWriter.getClass} STARTED ..........")
    var filepath = optionsIn.options.get("filepath").get.value.toString
    // var filepath1="C:\\hyderabad_team\\akka-applications\\src\\main\\resources\\L1.out"
    //var d = dataframe

    //dataframe.coalesce(1).write.mode("overwrite").format("com.databricks.spark.csv").save(filepath)
    dataframe.createOrReplaceTempView("quotes")

    val paidrateDF = com.diwo.mdbase.dataprep.module.ModuleApp.spark.sql(
      """
        |select Class,agedriver,GenderDriver,make,AgeYoungest,Substate,cresta_zone,risklevel,brand,OwnerAge_Unc,cover,state,AnnKMtrvl,week_ending,postcode,channel,Suburb,CAL_MONTH,FY_Year,NVIC,
        |COUNT(*) as NumberofQuotes,
        |SUM(Conversions) as NumberofConversions ,
        |(select count(RETAIL_PREMIUM_PAID) as paidcount from quotes where RETAIL_PREMIUM_PAID IS NOT NULL) as NumberofPaidQuotes ,
        |SUM(BASIC_PREMIUM_QUOTED) SumofPremiumsofallNBOffers,
        |(select SUM(RETAIL_PREMIUM_PAID) as Premiumspaid  from quotes where RETAIL_PREMIUM_PAID IS NOT NULL) as SumofPremiumsofallNBPaid
        | FROM quotes
        | GROUP BY Class,agedriver,GenderDriver,make,AgeYoungest,Substate,cresta_zone,risklevel,brand,OwnerAge_Unc,cover,state,AnnKMtrvl,week_ending,postcode,channel,Suburb,CAL_MONTH,FY_Year,NVIC
      """.stripMargin)

    paidrateDF.show(true)

    var t1 = System.nanoTime()
    var result = t1-t0
    ModuleApp.logger.info(s"TASK: ${QuotesAggr.getClass} SUCCESFULLY DONE IN JUST $result")
    var finalResult = t1-ModuleApp.tstart
    ModuleApp.logger.info(s"TOTAL TAKEN :   $finalResult")
    println("PROCESS SUCCESFULLY COMPLETED")
    //TODO as the Caller requries Optional(dataframe)
    Option(paidrateDF)
  }


  TasksRegistryHandler.tasksRegistry.put("quotesaggr", Quotesaggr)

}
