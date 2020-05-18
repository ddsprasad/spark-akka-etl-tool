/** PipelineManager
  *
  * Copyright (c) 2018.  Loven Systems - All Rights Reserved
  *
  * Unauthorized copying of this file, via any medium is strictly
  * prohibited
  *
  * Given a pipeline specification object, this actor creates the source,
  * sinks, and transformer actors, and creates a pipeline object used to
  * route the dataframe flow in the pipeline
  *

  */

package com.diwo.dataframe.pipeline

import akka.actor._
import com.diwo.mdbase.dataprep.meta.Messages._

import scala.collection.immutable.HashMap
import scala.collection.mutable
import scala.collection.mutable.ListBuffer

class PipelineManager() extends Actor {

  var pipelineRegistry = HashMap.empty[String, Tuple2[String, Seq[ActorRef]]]
  var pipeline = new ListBuffer[ActorRef]


  private def createPipeline(id: String, nodes: Int): Seq[ActorRef] = {

    InitSourceSinks

    for (i <- 1 to nodes) {
      //create pipeline node actor and add to the pipeline
      pipeline += context.actorOf(Props(new PipelineNode()), id + i)
    }
    pipeline
  }

  private def runPipeline(id: String, pipeline: Seq[ActorRef], tasks: Seq[AnyRef]) = {
    //check for empty pipeline
    val firstNodeActor = pipeline.head
    var newPipeline: mutable.Seq[ActorRef] = pipeline.tail.asInstanceOf[mutable.Seq[ActorRef]]
    firstNodeActor forward Run_Task(id, newPipeline, tasks.asInstanceOf[mutable.Seq[PipelineNodeTask]])
  }

  def receive = {

    case CreatePipeline(pipelineId: String, nodes: Int) =>

      // check for the existence of id
      if (pipelineRegistry.contains(pipelineId)) {
        sender() ! Pipeline_Error(pipelineId, "creation", "A pipeline already exists for pipelineId " + pipelineId)
      } else {
        // create pipeline
        val pipeline = createPipeline(pipelineId, nodes)

        //add pipeline to registry with state='ready'
        pipelineRegistry += (pipelineId -> ("ready", pipeline))

        //send message to sender
        sender() ! Pipeline_Ready(pipelineId)
      }

    case Run_Pipeline(pipelineId: String, tasks: Seq[AnyRef]) =>

      // get pipeline from registry
      if (pipelineRegistry.contains(pipelineId)) {
        // pipeline state must be ready - concurrent runs are not supported yet
        if (pipelineRegistry(pipelineId)._1.equals("ready")) {

          val pipeline = pipelineRegistry(pipelineId)._2

          // update the state of pipeline to running - look for proper method
          pipelineRegistry -= (pipelineId)
          pipelineRegistry += (pipelineId -> ("running", pipeline.asInstanceOf[Seq[ActorRef]]))


          //run pipeline
          runPipeline(pipelineId, pipeline, tasks)
        } else {
          sender() ! Pipeline_Error(pipelineId, "run", "Pipeline =" + pipelineId + " is not ready for a run")
        }
      } else {
        sender() ! Pipeline_Error(pipelineId, "run", "No pipeline exists for pipelineId " + pipelineId)
      }

    case Delete_Pipeline(pipelineId: String) =>

      val pipeline = pipelineRegistry(pipelineId)._2
      // for each actor in the pipeline, send a kill message 
      // TBD - this will kill all actors in the pipeline

      // delete pipeline from registry
      pipelineRegistry -= (pipelineId)

    // notfiy sender of deletion, if required

    case Pipeline_Run_Completed(pipelineId: String) =>
      // TBD - delete pipeline if it was created for just one run, else

      // change the state of pipeline from 'running' to 'ready'
      val pipeline = pipelineRegistry(pipelineId)._2
      pipelineRegistry -= (pipelineId)
      pipelineRegistry += (pipelineId -> ("ready", pipeline))

    // TBD - notify requestor of pipeline run completion
    // This will require keeping track of client

  }
}