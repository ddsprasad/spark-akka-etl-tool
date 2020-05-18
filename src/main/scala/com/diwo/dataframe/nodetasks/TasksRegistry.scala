/** TasksRegistry.scala
  *
  * Copyright (c) 2018.  Loven Systems - All Rights Reserved
  *
  * Unauthorized copying of this file, via any medium is strictly
  * prohibited
  *
  * Keeps track of all dataframe transformation tasks which could be dynamically
  * assigned to pipeline actors at run time. Thes tasks are parameterized function
  * objects
  *
  */

package com.diwo.dataframe.nodetasks

import scala.collection.mutable


object TasksRegistryHandler {

  var tasksRegistry: mutable.HashMap[String, Any] = mutable.HashMap.empty[String, Any]

}



