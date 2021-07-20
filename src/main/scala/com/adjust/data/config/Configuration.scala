package com.adjust.data.config
import scala.util.Properties

object Configuration {
  val JDBC_DRIVER: String = "org.postgresql.Driver"
  val DB_USER: String = Properties.envOrElse("DB_USER","postgres")
  val DB_PASSWORD: String = Properties.envOrElse("DB_PASSWORD","postgres")
  val DB_SCHEMA: String =  Properties.envOrElse("DB_SCHEMA","weather_data")
  val DB_HOST: String = Properties.envOrElse("DB_HOST","localhost:5432")
  val URL = s"jdbc:postgresql://$DB_HOST/$DB_SCHEMA"
  val DB_URL = s"$URL?user=$DB_USER&password=$DB_PASSWORD"
  val TABLE_NAME: String = "weather_balloon_data"

}
