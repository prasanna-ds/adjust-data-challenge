package com.adjust.data.models

import java.lang.reflect.Constructor
import scala.collection.immutable.ListMap

trait ListMapper[T <: ListMapper[T]] {
  val constructor: Array[Constructor[_]] = this.getClass.getConstructors

  def map(values: List[Any]): T =
    constructor(0).newInstance(values map {
      _.asInstanceOf[AnyRef]
    }: _*).asInstanceOf[T]
}

case class WeatherBalloonHeader(
                                 offsetStart: String = "",
                                 id: String = "",
                                 year: String = "",
                                 month: String = "",
                                 day: String = "",
                                 hour: String = "",
                                 releaseTime: String = "",
                                 numberOfLevels: String = "",
                                 pressureSourceCode: String = "",
                                 nonPressureSourceCode: String = "",
                                 latitude: String = "",
                                 longitude: String = ""
                               ) extends ListMapper[WeatherBalloonHeader]

case class WeatherBalloonData(
                               majorLevelType: String = "",
                               minorLevelType: String = "",
                               elapsedTimeSinceLaunch: String = "",
                               pressure: String = "",
                               pressureFlag: String = "",
                               geoPotentialHeight: String = "",
                               geoPotentialHeightFlag: String = "",
                               temperature: String = "",
                               temperatureProcessingFlag: String = "",
                               relativeHumidity: String = "",
                               dewPointDepression: String = "",
                               windDirection: String = "",
                               windSpeed: String = ""
                             ) extends ListMapper[WeatherBalloonData]

object WeatherBalloon {
  // Data positions to process from Input File
  // ORDER MUST BE MAINTAINED
  val headerDataOffsets: ListMap[String, Int] = ListMap(
    "OFFSET_START" -> 0,
    "ID" -> 1,
    "SOUNDING_DATE" -> 12,
    "YEAR" -> 17,
    "MONTH" -> 20,
    "DAY" -> 23,
    "HOUR" -> 26,
    "RELEASE_TIME" -> 31,
    "NUMBER_OF_LEVELS" -> 36,
    "PRESSURE_SOURCE_CODE" -> 45,
    "NON_PRESSURE_SOURCE_CODE" -> 54,
    "LATITUDE" -> 62,
    "LONGITUDE" -> 71
  )

  val dataRecordOffsets: ListMap[String, Int] = ListMap(
    // Data positions to process from Input File
    // ORDER MUST BE MAINTAINED
    "OFFSET_START" -> 0,
    "MAJOR_LEVEL_TYPE" -> 1,
    "MINOR_LEVEL_TYPE" -> 2,
    "ELAPSED_TIME_SINCE_LAUNCH" -> 8,
    "PRESSURE" -> 15,
    "PRESSURE_FLAG" -> 16,
    "GEO_POTENTIAL_HEIGHT" -> 21,
    "GEO_POTENTIAL_HEIGHT_FLAG" -> 22,
    "TEMPERATURE" -> 27,
    "TEMPERATURE_PROCESSING_FLAG" -> 28,
    "RELATIVE_HUMIDITY" -> 33,
    "DEW_POINT_DEPRESSION" -> 39,
    "WIND_DIRECTION" -> 45,
    "WIND_SPEED" -> 51
  )
}