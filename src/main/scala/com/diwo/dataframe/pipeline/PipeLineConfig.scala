package com.diwo.dataframe.pipeline

/**
  * PipeLineConfig    @version
  *

  *
  * A Class to represent 
  *
  *
  */

import com.diwo.mdbase.dataprep.meta.Messages._
import com.diwo.mdbase.dataprep.module.ModuleApp

import scala.io.Source

object PipeLineConfig {

  import scala.collection.mutable.HashMap

  //  case class NodeTaskOptionValue(value:AnyVal)
  //  case class NodeTaskOption(name:String, value:NodeTaskOptionValue)
  //  case class NodeTaskOptions(options: HashMap[String,NodeTaskOptionValue])
  //  case class PipelineNodeTask(taskname: String, options: NodeTaskOptions)
  //  case class PipelineSpec(spec:Seq[PipelineNodeTask])

  object NodeTaskOptionValueBuilder {
    def buildFromString(ofType: String, fromString: String): NodeTaskOptionValue = {
      ofType match {
        case "Int" => NodeTaskOptionValue(Integer.parseInt(fromString))
        case "String" => NodeTaskOptionValue(fromString.asInstanceOf[AnyVal])
        case _ => NodeTaskOptionValue(fromString.asInstanceOf[AnyVal])
      }

    }
  }

  //val test1=NodeTaskOptionValueBuilder.buildFromString("5")
  object NodeTaskOptionBuilder {
    def buildFromString(fromString: String): NodeTaskOption = {
      // extract name, type , and value
      println("In Option Builder")
      // println(fromString)
      var fromTrim = fromString.trim()
      val index = fromTrim.indexOf(":")
      // println("index="+index)
      val name = fromTrim.substring(0, index).trim()
      // println(" name ="+ name)
      val rest = fromTrim.substring(index).trim()
      println(" rest =" + rest)
      val index2 = rest.indexOf(" ")
      val optype = rest.substring(1, index2).trim()
      val opval = rest.substring(index2).trim()
      // println(" name = "+ name+" type ="+ optype+" value ="+ opval)

      val optVal = NodeTaskOptionValueBuilder.buildFromString(optype, opval)
      println(optVal)
      NodeTaskOption(name, optVal)
    }
  }

  object NodeTaskOptionsBuilder {
    def buildFromString(fromString: String): NodeTaskOptions = {
      println("in options builder")
      val optionSpecs: Array[String] = fromString.trim().split("Option")
      var myOptions: HashMap[String, NodeTaskOptionValue] = HashMap()
      for (opt <- optionSpecs) {
        if (opt != "") {
          println(opt)
          //col1:String value
          val rapt = NodeTaskOptionBuilder.buildFromString(opt)
          // val option1 =NodeTaskOptionValueBuilder.buildFromString("5")
          myOptions += (rapt.name -> rapt.value)
        }

      }

      println(myOptions.mkString(","))
      NodeTaskOptions(myOptions)
    }
  }

  // val testString:String="Option col1:String col_1 Option col2:Int 55"
  // println("defined test string")

  // val optionSpecs: Array[String] = testString.trim().split("Option" | "option" | "OPTION")
  //  val test2 = NodeTaskOptionsBuilder.buildFromString(testString)
  object PipelineNodeTaskBuilder {
    def buildFromString(fromString: String): PipelineNodeTask = {
      var fromTrim = fromString.trim()
      val index = fromTrim.indexOf("Option")
      // println("index="+index)
      val name = fromTrim.substring(0, index).trim()
      fromTrim = fromTrim.substring(index).trim()
      var opts = NodeTaskOptionsBuilder.buildFromString(fromTrim)
      PipelineNodeTask(name, opts)
    }
  }

  object PipelineSpecBuilder {
    def buildFromFile(filename: String): PipelineSpec = {
      val fromString = "read file as a string"
      buildFromString(fromString)
    }

    def buildFromString(fromString: String): PipelineSpec = {
      // extract tasks
      println("In Pipeline Spec Builder")

      // var spec: Seq[PipelineNodeTask] =Seq.empty
      val tasks: Array[String] = fromString.trim().split("Task")
      //ignore the first entry before first task key word
      var length = tasks.length
      println(length)
      var taskSpecs: Array[PipelineNodeTask] = new Array[PipelineNodeTask](length)
      var index = 0
      for (task <- tasks) {
        if (!task.equals("")) {
          println(task)
          val tt = PipelineNodeTaskBuilder.buildFromString(task)

          taskSpecs(index) = tt
          index = index + 1
          // println(tt)
          // spec = spec +: tt
        }
      }
      val tastkSpecs1 = taskSpecs.take(index)

      PipelineSpec(taskSpecs.take(index))

    }

  }

  var specAsString: String = "Task csvloader option filename:String testfile Task colselector Option col1:String col_1 Option col2:String col_2 "
  var specAsString2 = ""
  //var fname = "E:\\DataPrep-Spark\\src\\main\\resources\\join.conf"
  var fname = ModuleApp.confFile
    //"E:\\Learnings\\akka-applications-3318-Test\\src\\main\\resources\\pipeline.conf"
  for (lines <- Source.fromFile(fname).getLines) {
    specAsString2 += " " + lines
  }

  val y = PipelineSpecBuilder.buildFromString(specAsString2)

}
