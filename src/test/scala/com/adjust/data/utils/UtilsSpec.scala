package com.adjust.data.utils

import com.adjust.data.models.WeatherBalloon.{dataRecordOffsets, headerDataOffsets}
import com.adjust.data.models.{WeatherBalloonData, WeatherBalloonHeader}
import org.scalatest.flatspec.AnyFlatSpec
import com.adjust.data.utils.Utils._

class UtilsSpec extends AnyFlatSpec {

  val testHeaderRecord = "#USM00070219 1940 10 18 99 2200    7          cdmp-usm  607850 -1618400"
  val testDataRecord = "31 -9999  -9999    40 -9999 -9999 -9999    23    20 "

  val testParsedHeader: List[String] = testHeaderRecord.split(headerDataOffsets.values.toList)
  val testParsedData: List[String] = testDataRecord.split(dataRecordOffsets.values.toList)

  val testHeader: WeatherBalloonHeader = WeatherBalloonHeader().map(testParsedHeader)
  val testData: WeatherBalloonData = WeatherBalloonData().map(testParsedData)

  assert( testParsedHeader.length == 12)
  assert( testParsedData.length == 13)

  assert(testHeader.id == "USM00070219")
  assert(testHeader.year == "1940")
  assert(testHeader.month == "10")
  assert(testHeader.day == "18")
  assert(testHeader.hour == "99")
  assert(testHeader.releaseTime == "2200")
  assert(testHeader.pressureSourceCode == "")
  assert(testHeader.nonPressureSourceCode == "cdmp-usm")
  assert(testHeader.latitude == "607850")
  assert(testHeader.longitude == "-1618400")

  assert(testData.majorLevelType == "3")
  assert(testData.minorLevelType == "1")
  assert(testData.elapsedTimeSinceLaunch == "-9999")
  assert(testData.pressure == "-9999")
  assert(testData.pressureFlag == "")
  assert(testData.geoPotentialHeight == "40")
  assert(testData.geoPotentialHeightFlag == "")
  assert(testData.temperature == "-9999")
  assert(testData.temperatureProcessingFlag == "")
  assert(testData.relativeHumidity == "-9999")
  assert(testData.dewPointDepression == "-9999")
  assert(testData.windDirection == "23")
  assert(testData.windSpeed == "20")
}
