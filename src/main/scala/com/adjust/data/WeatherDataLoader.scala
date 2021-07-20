package com.adjust.data

import com.adjust.data.config.Configuration._
import com.adjust.data.models.WeatherBalloon._
import com.adjust.data.models._
import com.adjust.data.utils.Utils._
import com.typesafe.scalalogging.LazyLogging

import java.io.File
import java.sql.{Connection, DriverManager, PreparedStatement, SQLException}
import scala.io.Source


object WeatherDataLoader extends LazyLogging{

  def main(args: Array[String]): Unit = {

    var inputFilePath = ""
    args.sliding(2, 2).toList.collect {
      case Array("--inputPath", argInputPath: String) => inputFilePath = argInputPath
    }

    logger.info("Retrieving the list of files to load to database...")
    val files = new File(inputFilePath)
      .listFiles
      .filter(f => f.isFile && f.getName.endsWith(".txt"))

    var dbConnection: Connection = null
    var preparedStmt: PreparedStatement = null

    try {

      logger.info("Connecting to the database...")
      Class.forName(JDBC_DRIVER)
      dbConnection = DriverManager.getConnection(DB_URL)
      dbConnection.setAutoCommit(true)

      files.foreach { file: File =>
        val fileName = file.getAbsolutePath

        logger.info(s"Reading file $fileName...")
        val fileSource = Source.fromFile(fileName)
        val lines: Iterator[String] = fileSource.getLines
        var headerRecord: WeatherBalloonHeader = null
        var dataRecord: WeatherBalloonData = null
        var dataRecordCount: Int = 0

        val insertSql =
          s"""
             |insert into $TABLE_NAME (id,
             |                   sounding_date,
             |                   hour,
             |                   release_time,
             |                   number_of_levels,
             |                   pressure_source_code,
             |                   non_pressure_source_code,
             |                   latitude,
             |                   longitude,
             |                   major_level_type,
             |                   minor_level_type,
             |                   elapsed_time_since_launch,
             |                   pressure,
             |                   pressure_flag,
             |                   geo_potential_height,
             |                   geo_potential_height_flag,
             |                   temperature,
             |                   temperature_processing_flag,
             |                   relative_humidity,
             |                   dew_point_depression,
             |                   wind_direction,
             |                   wind_speed)
             |values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                      """.stripMargin
        preparedStmt = dbConnection.prepareStatement(insertSql)

        lines.foreach { l =>
          if (l.startsWith("#")) {
            headerRecord = WeatherBalloonHeader().map(l.split(headerDataOffsets.values.toList))
          }
          else {
            dataRecord = null
            dataRecord = WeatherBalloonData().map(l.split(dataRecordOffsets.values.toList))

            // header record
            preparedStmt.setString(1, headerRecord.id)
            preparedStmt.setInt(2, (headerRecord.year + headerRecord.month + headerRecord.day).toInt)
            preparedStmt.setInt(3, headerRecord.hour.toInt)
            preparedStmt.setInt(4, headerRecord.releaseTime.toInt)
            preparedStmt.setInt(5, headerRecord.numberOfLevels.toInt)
            preparedStmt.setString(6, headerRecord.pressureSourceCode)
            preparedStmt.setString(7, headerRecord.nonPressureSourceCode)
            preparedStmt.setInt(8, headerRecord.latitude.toInt)
            preparedStmt.setInt(9, headerRecord.longitude.toInt)

            // data record
            preparedStmt.setInt(10, dataRecord.majorLevelType.toInt)
            preparedStmt.setInt(11, dataRecord.minorLevelType.toInt)
            preparedStmt.setInt(12, dataRecord.elapsedTimeSinceLaunch.toInt)
            preparedStmt.setInt(13, dataRecord.pressure.toInt)
            preparedStmt.setString(14, dataRecord.pressureFlag)
            preparedStmt.setInt(15, dataRecord.geoPotentialHeight.toInt)
            preparedStmt.setString(16, dataRecord.geoPotentialHeightFlag)
            preparedStmt.setInt(17, dataRecord.temperature.toInt)
            preparedStmt.setString(18, dataRecord.temperatureProcessingFlag)
            preparedStmt.setInt(19, dataRecord.relativeHumidity.toInt)
            preparedStmt.setInt(20, dataRecord.dewPointDepression.toInt)
            preparedStmt.setInt(21, dataRecord.windDirection.toInt)
            preparedStmt.setInt(22, dataRecord.windSpeed.toInt)
            preparedStmt.addBatch()

            dataRecordCount += 1

            if (dataRecordCount == headerRecord.numberOfLevels.toInt) {
              headerRecord = null
              dataRecordCount = 0
              preparedStmt.executeBatch()
            }
          }
        }
      }
    } catch {
      case se: SQLException => logger.error(se.getMessage)
      case e: Exception => logger.error(e.getMessage)
    } finally {
      if (preparedStmt != null) preparedStmt.close()
      if (dbConnection != null) dbConnection.close()
    }
  }
}
