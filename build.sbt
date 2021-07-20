name := "adjust-data-challenge"
organization := "com.adjust.data"
version := "1.0"
scalaVersion := "2.12.10"
inThisBuild(List(assemblyJarName in assembly := "adjust-data-challenge.jar"))
libraryDependencies ++= {
  sys.props += "packaging.type" -> "jar"
  Seq(
    "org.scalatest"              %% "scalatest"     % "3.2.9",
    "com.typesafe.scala-logging" %% "scala-logging" % "3.9.4",
    "org.apache.spark"           %% "spark-sql"     % "3.1.2",
    "org.apache.spark"           %% "spark-core"    % "3.1.2",
    "org.apache.hadoop"           % "hadoop-hdfs"   % "3.2.1",
    "org.postgresql"              % "postgresql"    % "42.2.4",
  )
}

fork in run := true
assemblyMergeStrategy in assembly := {
  case "META-INF/services/org.apache.spark.sql.sources.DataSourceRegister" => MergeStrategy.concat
  case PathList("META-INF", "services", "org.apache.hadoop.fs.FileSystem") => MergeStrategy.filterDistinctLines
  case PathList("META-INF", xs @ _*)                                       => MergeStrategy.discard
  case _                                                                   => MergeStrategy.first
}