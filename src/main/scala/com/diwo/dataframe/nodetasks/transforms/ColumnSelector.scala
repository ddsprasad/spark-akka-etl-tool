package com.diwo.dataframe.nodetasks.transforms

import com.diwo.dataframe.nodetasks.TasksRegistryHandler
import com.diwo.mdbase.dataprep.meta.Messages._
import com.diwo.mdbase.dataprep.module.ModuleApp
import org.apache.spark.sql.DataFrame

/** ColumnSelector.scala
  *
  * Copyright (c) 2018.  Loven Systems - All Rights Reserved
  *
  * Unauthorized copying of this file, via any medium is strictly
  * prohibited
  *
  * A task function object that takes a dataframe
  * and returns a new dataframe with specifed columns in the options
  *
  * The task object is also registered in a task registry from which it can be
  * searched by name
  *
  *
  */

object ColumnSelector {
  val colSelector = (optionsIn: NodeTaskOptions, dataFrame: DataFrame) => {

    var t0 = System.nanoTime()
    ModuleApp.logger.info(s"TASK: ${ColumnSelector.getClass} STARTED ..........")
    var colnames = optionsIn.options.get("colnames")
    var dataframeselected = dataFrame

    colnames match {
      case Some(b: NodeTaskOptionValue) =>
        val a: List[String] = b.value.toString.split(",").toList
        dataframeselected = dataFrame
      case Some(a: List[String]) =>
        dataframeselected = dataFrame
    }
    var t1 = System.nanoTime()
    var result = t1-t0
    ModuleApp.logger.info(s"TASK: ${ColumnSelector.getClass} SUCCESFULLY DONE IN JUST $result")
    Option(dataframeselected) //   newDataFrame = dataframe.cols().select(options("colnames"))
  }

  TasksRegistryHandler.tasksRegistry.put("colselector", colSelector)
}

// inputDataFrame.cols().select("BRND_CD",   "SHPG_TRXN_LN_DT", "NET_SALES", 
// "NET_SLS_QTY")