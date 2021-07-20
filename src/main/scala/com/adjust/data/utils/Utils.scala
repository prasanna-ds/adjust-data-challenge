package com.adjust.data.utils

import org.apache.spark.sql.{DataFrame, SparkSession}

import java.util.Properties

object Utils {
  implicit class StringSplit(s: String) {
    def split(indices: List[Int]): List[String] = {
      (indices zip indices.tail).map { case (a, b) =>
        s.substring(a, b).trim
      }
    }
  }

  val APP_NAME = "weather-balloon-data-processor"

  /*
  Create Spark Session
   */
  def createSparkSession: SparkSession = {
    val sparkSession = SparkSession
      .builder()
      .appName(APP_NAME)
      .master("local[*]")
      .getOrCreate()

    val hadoopConfig = sparkSession.sparkContext.hadoopConfiguration
    hadoopConfig.set("fs.hdfs.impl", classOf[org.apache.hadoop.hdfs.DistributedFileSystem].getName)
    hadoopConfig.set("fs.file.impl", classOf[org.apache.hadoop.fs.LocalFileSystem].getName)

    sparkSession
  }

  /**
   * Reads data from JDBC Source
   * @param url JDBC URL
   * @param dbtable DB Table
   * @param numPartitions Number of partitions to be used for parallelism in reading
   * @param fetchsize Number of rows to fetch per round trip
   * @return
   */
  def readFromJdbc(url: String,
                   dbtable: String,
                   connectionProperties: Properties,
                   numPartitions: Int = 10,
                   fetchsize: Long = 1000000L // The values are tuned as per the system and local run.
                  ): DataFrame = {
    val sourceDf = createSparkSession.read
      .option("url", url)
      .option("dbtable", dbtable)
      .option("numPartitions", numPartitions)
      .option("fetchsize", fetchsize)
      .jdbc(url = url, table = dbtable, properties = connectionProperties)
    sourceDf
  }

  /**
   * Write a dataframe as Parquet file
   * @param dataFrame Dataframe to write
   * @param partitionColumn Column to be partitioned by
   * @param outputLocation Location of the output
   */
  def writeAsParquet(dataFrame: DataFrame, partitionColumn: String, outputLocation: String): Unit = {
    dataFrame
      .write
      .partitionBy(partitionColumn)
      .parquet(outputLocation)
  }
}
