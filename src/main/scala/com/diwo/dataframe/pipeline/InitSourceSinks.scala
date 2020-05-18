package com.diwo.dataframe.pipeline

import com.diwo.dataframe.nodetasks.TasksRegistryHandler
import com.diwo.dataframe.nodetasks.aggregations.{DynamicGroupBy, QuotesAggr}
import com.diwo.dataframe.nodetasks.sinks.{CSVWriter, ESWriter, ESwriterCustomeID}
import com.diwo.dataframe.nodetasks.sources._
import com.diwo.dataframe.nodetasks.transforms.ColumnSelector

/**
  * InitSourceSinks    @version
  *

  *
  *
  * A Class to represent 
  *
  *
  */
object InitSourceSinks {
  TasksRegistryHandler
  CSVWriter
  ColumnSelector
  CSVLoader
  ESWriter
  QuotesAggr
  ESwriterCustomeID
  DynamicGroupBy
}
