
/** CSVLoader.scala
  *
  * Copyright (c) 2018.  Loven Systems - All Rights Reserved
  *
  * Unauthorized copying of this file, via any medium is strictly
  * prohibited
  *
  * A task function object that loads a csvfile into a dataframe
  * and returns reference to the dataframe
  *
  * The task object is also registered in a task registry from which it can be
  * searched by name


  *
  */

package com.diwo.dataframe.nodetasks.sources

import com.diwo.dataframe.nodetasks.TasksRegistryHandler
import com.diwo.mdbase.dataprep.meta.Messages.NodeTaskOptions
import com.diwo.mdbase.dataprep.module.ModuleApp.sqlContext


object CSVLoader {

  val csvLoader = (optionsIn: NodeTaskOptions) => {

    var colnames = optionsIn.options.get("colnames")

    var srcPath: String = optionsIn.options.get("filepath").get.value.toString
    //     fold("")(_.toString)
    //srcPath ="C:\\hyderabad_team\\akka-applications\\src\\main\\resources\\L1.csv"

    val dataFrame =    sqlContext.read.format("com.databricks.spark.csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .option("delimiter", ",")
      .load(srcPath)
    //dataFrame.show(true)

    Option(dataFrame)
    //  dataFrame.out().print()
  }

  TasksRegistryHandler.tasksRegistry.put("csvloader", csvLoader)
}