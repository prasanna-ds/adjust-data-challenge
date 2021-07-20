package com.adjust.data

import com.adjust.data.config.Configuration.{DB_PASSWORD, DB_USER, TABLE_NAME, URL}
import com.adjust.data.utils.Utils.{readFromJdbc, writeAsParquet}
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col

import java.util.Properties

object WeatherDataProcessor extends LazyLogging {

  def main(args: Array[String]): Unit = {

    try {
      var outputPath = ""
      args.sliding(2, 2).toList.collect {
        case Array("--outputPath", argOutputPath: String) => outputPath = argOutputPath
      }

      val connectionProperties = new Properties()
      connectionProperties.put("user", DB_USER)
      connectionProperties.put("password", DB_PASSWORD)

      logger.info(s"Reading $TABLE_NAME from JDBC source...")
      val weatherDataDf = readFromJdbc(URL, TABLE_NAME, connectionProperties)

      // Calculate the bucket where the data is going to fall into.
      // Data will be partitioned to be in thousands of meters altitude
      logger.info("Processing weather balloon data...")
      val processedWeatherDataDf = processWeatherData(weatherDataDf)
        .repartition(col("altitude"))

      logger.info(s"Writing processed weather balloon data into $outputPath...")
      writeAsParquet(processedWeatherDataDf, "altitude", outputPath)
    } catch {
      case e: Exception => logger.error(e.getMessage)
    }
  }

  def processWeatherData(weatherDataDf: DataFrame): DataFrame = {
    weatherDataDf
      .withColumn("altitude", (col("geo_potential_height") divide 1000).cast("int") * 1000)
  }

}
