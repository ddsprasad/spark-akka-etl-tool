/** ModuleApp.scala
  *
  *
  * Copyright (c) 2018.  Loven Systems - All Rights Reserved
  *
  * Unauthorized copying of this file, via any medium is strictly
  * prohibited
  *
  * This is the main app for the data preparation sub-module of mdbase.
  * The app creates the actor system and the top level actor BaseDataServer
  * which gets the ball rolling
  */

package com.diwo.mdbase.dataprep.module

import java.io.{FileOutputStream, PrintStream}

import akka.actor.{ActorSystem, Props}
import com.diwo.mdbase.dataprep.meta.ActorEnum
import com.diwo.mdbase.dataprep.meta.Messages.Start_Module
import grizzled.slf4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
//import org.apache.log4j.{Level, Logger}

object ModuleApp extends App {

  val logger = Logger("com.diwo.mdbase.dataprep.module")
val confFile = args(0).toString
  //val  esMapping = args(1).toString

var tstart = System.nanoTime()
val conf = new SparkConf()
  conf.setMaster("local[*]")
  conf.set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
  conf.set("spark.debug","maxToStringFields")
  conf.set("spark.sql.crossJoin.enabled", "true")
  conf.set("spark.kryoserializer.buffer.max","768m")
  conf.set("spark.sql.codegen","true")
  conf.set("spark.executor.memory", "3g")
  conf.set("spark.debug.maxToStringFields","100000")
 // conf.set("es.mapping.id",esMapping)
  //conf.set("es.mapping.id","CUSTID")

  //Spark session
  val spark = SparkSession.builder()
   // .config("spark.sql.warehouse.dir", "E:\\Vdiwo\\sparkWarehouse")
    .config(conf)
    .getOrCreate()
 // System.setProperty("hadoop.home.dir", "C:\\winutils")

  import spark.sqlContext.implicits._
  val sqlContext = spark.sqlContext


  implicit val system = ActorSystem("MDbaseDataPrepSystem")
  private val appStartActor =
    system.actorOf(Props(new ModuleAppStarter()), ActorEnum.MD_DATAPREP_MODULE_STARTER)

  // start module
  appStartActor ! Start_Module

}