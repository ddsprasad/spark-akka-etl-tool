/** CSVWriter.scala
  *
  * Copyright (c) 2018.  Loven Systems - All Rights Reserved
  *
  * Unauthorized copying of this file, via any medium is strictly
  * prohibited
  *
  * A task function object that writes dataframe to a csvfile
  *
  * The task object is also registered in a task registry from which it can be
  * searched by name
   *
  */

package com.diwo.dataframe.nodetasks.sinks

import com.diwo.dataframe.nodetasks.TasksRegistryHandler
import com.diwo.mdbase.dataprep.meta.Messages.NodeTaskOptions
import com.diwo.mdbase.dataprep.module.ModuleApp
import org.apache.spark.sql.DataFrame

object CSVWriter {
  val csvWriter = (optionsIn: NodeTaskOptions, dataframe: DataFrame) => {
    var t0 = System.nanoTime()
    ModuleApp.logger.info(s"TASK: ${CSVWriter.getClass} STARTED ..........")
    var filepath = optionsIn.options.get("filepath").get.value.toString
    // var filepath1="C:\\hyderabad_team\\akka-applications\\src\\main\\resources\\L1.out"
    //var d = dataframe

    dataframe.coalesce(1).write.mode("overwrite").format("com.databricks.spark.csv").option("header",true).save(filepath)

    var t1 = System.nanoTime()
    var result = t1-t0
    ModuleApp.logger.info(s"TASK: ${CSVWriter.getClass} SUCCESFULLY DONE IN JUST $result")
    var finalResult = t1-ModuleApp.tstart
    ModuleApp.logger.info(s"TOTAL TAKEN :   $finalResult")
    println("PROCESS SUCCESFULLY COMPLETED")
    //TODO as the Caller requries Optional(dataframe)
    Option(dataframe)
  }


  TasksRegistryHandler.tasksRegistry.put("csvwriter", csvWriter)


}