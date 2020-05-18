/** messages.scala
  *
  * Copyright (c) 2018.  Loven Systems - All Rights Reserved
  *
  * Unauthorized copying of this file, via any medium is strictly
  * prohibited
  *
  * List of message classes used by MDBase Module
  *
  */

package com.diwo.mdbase.dataprep.meta
import org.apache.spark.sql.DataFrame

//import com.diwo.dataframe._

object Messages {

  import akka.actor.ActorRef

  import scala.collection.mutable._


  case class NodeTaskOptionValue(value: AnyVal)

  case class NodeTaskOption(name: String, value: NodeTaskOptionValue)

  case class NodeTaskOptions(options: HashMap[String, NodeTaskOptionValue])

  case class PipelineNodeTask(taskname: String, options: NodeTaskOptions)

  case class PipelineSpec(spec: Seq[PipelineNodeTask])

  //  case class NodeTaskOptions(options: HashMap[String, Any])

  //  case class PipelineNodeTask(taskname: String, options: NodeTaskOptions)

  // Message received by ModuleAppStarter
  case object Start_Module

  // Messages received by PipelineManager
  case class CreatePipeline(pipelineId: String, nodes: Int)

  case class Run_Pipeline(pipelineId: String, tasks: Seq[PipelineNodeTask])

  case class Delete_Pipeline(pipelineId: String)

  case class Pipeline_Run_Completed(pipelineId: String)

  // Messages received by PipelineNode
  case class Run_Task(pipelineId: String, pipeline: Seq[ActorRef], tasks: Seq[PipelineNodeTask], dataframe: Option[DataFrame] = None)

  // Messages received by DataPrepClient
  case class Pipeline_Ready(pipelineId: String)

  case class Pipeline_Error(pipelineId: String, errorcode: String, errormessage: String)


  case class Execute_Pipeline(sourceFilePath: String, sinkFilePath: String)

  case class Pipeline_Source(source: ActorRef)

  case class Load(inputFilePath: String, outPutFilePath: String)


  case class Run_Test(manager: ActorRef)

  case class Create_Pipeline()

}