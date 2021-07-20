package com.adjust.data

import com.adjust.data.WeatherDataProcessor.processWeatherData
import com.adjust.data.utils.Utils.createSparkSession
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.scalatest.flatspec.AnyFlatSpec

import java.util.{List => JavaList}
import scala.collection.JavaConverters._

class WeatherDataProcessorSpec extends AnyFlatSpec {

  val sparkSession: SparkSession = createSparkSession

  val schema: StructType = StructType(Array(
    StructField("geo_potential_height", IntegerType, nullable = true),
  ))
  val usersGeo: JavaList[Row] = Seq(
    Row(8), Row(100), Row(1100), Row(2200), Row(5500), Row(8999)
  ).asJava

  val testWeatherDataDF: DataFrame = sparkSession.createDataFrame(usersGeo, schema)
  val testProcessedWeatherDataDf: DataFrame = processWeatherData(testWeatherDataDF)

  "processedWeatherDf" should "have same count as the input dataframe" in {
    assert(testProcessedWeatherDataDf.count() == 6)
  }

  "processedWeatherDf" should "have the altitude column mapped to right buckets" in {
     val outputDf = testProcessedWeatherDataDf.collectAsList()

    assert(outputDf.get(0).getInt(0) == 8 && outputDf.get(0).getInt(1) == 0)
    assert(outputDf.get(1).getInt(0) == 100 && outputDf.get(1).getInt(1) == 0)
    assert(outputDf.get(2).getInt(0) == 1100 && outputDf.get(2).getInt(1) == 1000)
    assert(outputDf.get(3).getInt(0) == 2200 && outputDf.get(3).getInt(1) == 2000)
    assert(outputDf.get(4).getInt(0) == 5500 && outputDf.get(4).getInt(1) == 5000)
    assert(outputDf.get(5).getInt(0) == 8999 && outputDf.get(5).getInt(1) == 8000)
  }

}
