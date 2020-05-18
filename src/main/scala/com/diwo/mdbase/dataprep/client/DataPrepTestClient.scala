/** DataPrepTestClient.scala
  *
  *
  * Copyright (c) 2018.  Loven Systems - All Rights Reserved
  *
  * Unauthorized copying of this file, via any medium is strictly
  * prohibited
  *
  * Test client for building and running a pipeline
  */

package com.diwo.mdbase.dataprep.client

import akka.actor._
import com.diwo.dataframe.pipeline.PipeLineConfig
import com.diwo.mdbase.dataprep.meta.Messages._

import scala.collection.mutable

class DataPrepTestClient() extends Actor {

  var pipelineSpec = PipeLineConfig


  var pipelineManager: ActorRef = self

  def receive = {


    case Run_Test(manager: ActorRef) =>

      pipelineManager = manager

      /**
        *
        * Build first pipeline with two nodes:
        * First Node - for loading data from CSV file and selecting specified
        * columns
        * Second Node - for saving the transformed dataframe back into another csv file
        */

      pipelineManager ! CreatePipeline(pipelineId = "firstpipeline", nodes = 2)

    /**
      * Build a second pipeline with two nodes
      * First node for loading data from csv file
      * Second node for selecting specified columns & saving them in a csv file
      */

    //   pipelineManager ! CreatePipeline(pipelineId="secondpipeline", nodes= 2)

    /**
      * Build a third pipeline with three nodes
      * First node for loading data from csv file
      * Second node for selecting specified columns
      * Third node for saving selections in a csv file
      */

    //   pipelineManager ! CreatePipeline(pipelineId="thirdpipeline", nodes= 3)

    //    pipelineManager ! CreatePipeline(pipelineId="thirdpipeline", nodes= 3)

    case Pipeline_Ready(pipelineId: String) =>

      pipelineId match {
        case "firstpipeline" =>

          //            var options1: HashMap[String, Any]=HashMap.empty[String,Any]
          //            options1("filepath")= "C:\\hyderabad_team\\akka-applications\\src\\main\\resources\\L1.csv"
          //
          //            //TODO this column list is not being recognized by morpheus as it requires as individual elements in the call
          //            options1("colnames" )=(
          //              List("BRND_CD","SHPG_TRXN_LN_DT","NET_SALES","NET_SLS_QTY","RETURN_QTY"))
          //
          //
          //            var options2: HashMap[String, Any]=HashMap.empty[String,Any]
          //            options2("filepath") ="C:\\hyderabad_team\\akka-applications\\src\\main\\resources\\L1.out"
          //
          //            val tasks=   Seq(PipelineNodeTask("csvloader_colselector", NodeTaskOptions(options1)),
          //              PipelineNodeTask("csvwriter",NodeTaskOptions(options2)))
          val tasks: mutable.Seq[PipelineNodeTask] = pipelineSpec.y.spec

          pipelineManager ! Run_Pipeline(pipelineId, tasks)

        /*      case "secondpipeline" =>

                var options3: HashMap[String, Any]=HashMap.empty[String,Any]
                 options3("filepath") ="C:\\hyderabad_team\\akka-applications\\src\\main\\resources\\L1.out"
                var options4: HashMap[String, Any]=HashMap.empty[String,Any]
                options4("colnames" ) = List("BRND_CD","SHPG_TRXN_LN_DT","NET_SALES","NET_SLS_QTY")
                options4("filepath") ="C:\\hyderabad_team\\akka-applications\\src\\main\\resources\\L2.out"


                 val tasks= Seq(PipelineNodeTask("csvloader", NodeTaskOptions(options3)),
                   PipelineNodeTask("csvwriter_colselector",  NodeTaskOptions(options4)))

                 pipelineManager ! Run_Pipeline(pipelineId,tasks)

                case "thirdpipeline" =>

                  var options5: HashMap[String, Any]=HashMap.empty[String,Any]
                  options5("filepath") ="C:\\hyderabad_team\\akka-applications\\src\\main\\resources\\L2.out"
                  val options6: HashMap[String, Any]=HashMap.empty[String,Any]
                  options6("colnames" ) = List("NET_SALES","NET_SLS_QTY")

                  var options7: HashMap[String, Any]=HashMap.empty[String,Any]
                  options7("filepath") ="C:\\hyderabad_team\\akka-applications\\src\\main\\resources\\L3.out"

                val tasks= Seq(PipelineNodeTask("csvloader", NodeTaskOptions(options5)),
                  PipelineNodeTask("colselector", NodeTaskOptions(options6)),
                  PipelineNodeTask("csvwriter", NodeTaskOptions(options7)))
                pipelineManager ! Run_Pipeline(pipelineId,tasks)
           */

      }

  }
}