package com.diwo.dataframe.nodetasks.sinks

import com.diwo.dataframe.nodetasks.TasksRegistryHandler
import com.diwo.mdbase.dataprep.meta.Messages.NodeTaskOptions
import com.diwo.mdbase.dataprep.module.ModuleApp
import org.apache.spark.sql.DataFrame
import org.elasticsearch.spark.sql.EsSparkSQL

object ESwriterCustomeID {

  val eswriterCustomeID = (optionsIn: NodeTaskOptions, dataframe: DataFrame) => {
    var t0 = System.nanoTime()
    ModuleApp.logger.info(s"TASK: ${ESWriter.getClass} STARTED ..........")
    //var filepath = optionsIn.options.get("filepath").get.value.toString
    // var filepath1="C:\\hyderabad_team\\akka-applications\\src\\main\\resources\\L1.out"
    //var d = dataframe
    val es_node = optionsIn.options.get("es_node").get.value.toString
    val es_cluster_name = optionsIn.options.get("es_cluster_name").get.value.toString
    val es_index = optionsIn.options.get("es_index").get.value.toString
    val es_index_mapping = optionsIn.options.get("es_index_mapping").get.value.toString
    //val es_mapping_id = optionsIn.options.get("es_mapping_id").get.value.toString

    //dataframe.coalesce(1).write.mode("overwrite").format("com.databricks.spark.csv").save(filepath)
    val elasticConf: Map[String, String] = Map("es.nodes" -> es_node, "es.clustername" -> es_cluster_name)
    EsSparkSQL.saveToEs(dataframe,s"${es_index}/${es_index_mapping}",elasticConf)

    var t1 = System.nanoTime()
    var result = t1-t0
    ModuleApp.logger.info(s"TASK: ${ESwriterCustomeID.getClass} SUCCESFULLY DONE IN JUST $result")
    var finalResult = t1-ModuleApp.tstart
    ModuleApp.logger.info(s"TOTAL TAKEN :   $finalResult")
    println("PROCESS SUCCESFULLY COMPLETED")
    //TODO as the Caller requries Optional(dataframe)
    Option(dataframe)
  }


  TasksRegistryHandler.tasksRegistry.put("eswritercustomeid", eswriterCustomeID)

}
