name := "dataprep-spark"

version := "0.1"

scalaVersion := "2.11.0"

lazy val  sparkVersion = "2.0.0"

lazy val akkaVersion = "2.5.3"

libraryDependencies ++= Seq (
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "com.typesafe.akka" %% "akka-actor" % akkaVersion,
  "com.typesafe.akka" %% "akka-http" % "10.0.4",
  "com.typesafe.akka" %% "akka-testkit" % akkaVersion,
  "com.typesafe.akka" %% "akka-persistence" % akkaVersion,
  "org.iq80.leveldb" % "leveldb" % "0.7",
  "org.fusesource.leveldbjni" % "leveldbjni-all" % "1.8",
  "org.scalatest" %% "scalatest" % "3.0.1" % "test"
)
libraryDependencies += "org.elasticsearch" %% "elasticsearch-spark-20" % "5.5.0"
libraryDependencies += "org.elasticsearch" % "elasticsearch" % "5.5.0"
// https://mvnrepository.com/artifact/com.databricks/spark-csv
libraryDependencies += "com.databricks" %% "spark-csv" % "1.5.0"

libraryDependencies ++= Seq("org.slf4j" % "slf4j-api" % "1.7.5",
  "org.slf4j" % "slf4j-simple" % "1.7.5",
  "org.clapper" %% "grizzled-slf4j" % "1.3.2")

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}