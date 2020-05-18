/** PipelineNode.scala
  *
  * Copyright (c) 2018.  Loven Systems - All Rights Reserved
  *
  * Unauthorized copying of this file, via any medium is strictly
  * prohibited
  *
  * This is an actor which forms a dataframe
  * transformation node in a dataframe transformation
  * pipeline
  *
 Run_Task - receives a diwo dataframe, performs the assigned task.
  *          If not sink node, the transformed dataframe is then forwarded to
  *          the next node in pipeline.
  */

package com.diwo.dataframe.pipeline

import akka.actor._
import com.diwo.dataframe.nodetasks.TasksRegistryHandler
import com.diwo.mdbase.dataprep.meta.Messages
import com.diwo.mdbase.dataprep.meta.Messages.{NodeTaskOptions, PipelineNodeTask, Pipeline_Run_Completed, Run_Task}
import org.apache.spark.sql.DataFrame

import scala.collection.mutable

class PipelineNode() extends Actor {

  private def forwardToNextNode(pipelineId: String, pipeline: Seq[ActorRef],
                                tasks: Seq[PipelineNodeTask], dataframe: Option[DataFrame]): Unit = {

    if (tasks.isEmpty) {
      sender ! Pipeline_Run_Completed(pipelineId)
    }
    else {
      val nextNode = pipeline.head
      //
      //       //var newPipeline:mutable.Seq[ActorRef] = pipeline.tail.asInstanceOf[mutable.Seq[ActorRef]]
      var newPipeline: mutable.Seq[ActorRef] = pipeline.asInstanceOf[mutable.Seq[ActorRef]]
      if (newPipeline.isEmpty) {
        // I am the sink Node in the pipeline - no more forwarding
        sender ! Pipeline_Run_Completed(pipelineId)
      } else {
        //newPipeline = pipeline.tail.asInstanceOf[mutable.Seq[ActorRef]]
        nextNode forward Run_Task(
          pipelineId = pipelineId,
          pipeline = newPipeline.asInstanceOf[mutable.Seq[ActorRef]],
          dataframe = dataframe,
          tasks = tasks.asInstanceOf[mutable.Seq[Messages.PipelineNodeTask]] // remove my task
        )
      }
    }
  }

  def execTaskSomeDataframe(fun: (NodeTaskOptions, DataFrame) => Any, OptionsIn: NodeTaskOptions, dataframe: DataFrame): Any = {

    fun(OptionsIn, dataframe)
  }

  def execTaskOptionalDataframe(fun: (NodeTaskOptions, Option[DataFrame]) => Any, OptionsIn: NodeTaskOptions, dataframe: Option[DataFrame]): Any = {
    fun(OptionsIn, dataframe)
  }

  def execTask(fun: (NodeTaskOptions) => Any, OptionsIn: NodeTaskOptions): Any = {
    fun(OptionsIn)
  }

  def receive = {
    case Run_Task(pipelineId: String, pipeline: Seq[ActorRef],
    tasks: Seq[PipelineNodeTask], dataframe: Option[DataFrame]) =>
      if (tasks.head == null) {

      }
      val myTask = tasks.head
      val nodeOptions = myTask.options

      dataframe match {
        case AnyRef =>
          val myTaskFunction = TasksRegistryHandler.tasksRegistry(myTask.taskname).asInstanceOf[(NodeTaskOptions, Option[DataFrame]) => Any]

          // myTasks._2 provides options list for the task
          val transformedDataFrame = execTaskOptionalDataframe(myTaskFunction, nodeOptions, dataframe).asInstanceOf[Option[DataFrame]]
          forwardToNextNode(pipelineId, pipeline, tasks.tail, transformedDataFrame)
        case None =>
          // source task does not require data frame as a parameter
          val myTaskFunction = TasksRegistryHandler.tasksRegistry(myTask.taskname).asInstanceOf[(NodeTaskOptions) => Any]

          val newDataFrame: Any = execTask(myTaskFunction, nodeOptions)

          forwardToNextNode(pipelineId, pipeline, tasks.tail, newDataFrame.asInstanceOf[Option[DataFrame]])
        case _: Any =>
          val myTaskFunction = TasksRegistryHandler.tasksRegistry(myTask.taskname).asInstanceOf[(NodeTaskOptions, DataFrame) => Any]

          val transformedDataFrame = execTaskSomeDataframe(myTaskFunction, nodeOptions, dataframe.get).asInstanceOf[Option[DataFrame]]
          forwardToNextNode(pipelineId, pipeline, tasks.tail, transformedDataFrame)
        case _ =>

      }

    case _ => //do nothing for now
  }

}
  