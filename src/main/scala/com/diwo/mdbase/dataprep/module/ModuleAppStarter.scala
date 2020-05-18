/** ModuleAppStarter.scala
  *
  * Copyright (c) 2018.  Loven Systems - All Rights Reserved
  *
  * Unauthorized copying of this file, via any medium is strictly
  * prohibited
  *
  * This actor is the top level actor of the base data server
  * sub-module of MDBase. This actor creates two actors:
  * 1. TransformPipelineManager which is responsible for managing
  * data transformation pipeline, and
  * 2. transformPipelineExecutor which is responsible for executing
  * a pipeline
  *
  * In addition, this actor serves as the main interface to the
  * Base Data Server Module
  *
  *
  */

package com.diwo.mdbase.dataprep.module

import akka.actor._
import com.diwo.dataframe.pipeline._
import com.diwo.mdbase.dataprep.meta._
//import com.diwo.mdbase.dataprep.ActorEnum
import com.diwo.mdbase.dataprep.client._
import com.diwo.mdbase.dataprep.meta.Messages.{Run_Test, Start_Module}

class ModuleAppStarter() extends Actor {

  def receive = {
    case Start_Module =>
      // create pipeline manager
      val pipelineManager = context.actorOf(
        Props(new PipelineManager()),
        ActorEnum.MD_DATAPREP_PIPELINE_MANAGER)

      // also create a test client for testing for now
      val testClient = context.actorOf(
        Props(new DataPrepTestClient()),
        ActorEnum.MD_DATAPREP_TEST_CLIENT)

      Thread.sleep(2000)

      // send a message to start test
      testClient ! Run_Test(pipelineManager)
    case _ =>
  }
}