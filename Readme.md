# Overview

This project has two main methods,

WeatherDataLoader:

Loads the data into Postgres. The data will be loaded normalized into database considering 
all the data will be required and no efficient keys to perform a join during the data processing.

Args:
  --inputPath - A directory containing files to be loaded

WeatherDataProcessor:

It is a spark job that connects to the database, reads the data and buckets the data into thousands of
meters of altitude on column `geo_potential_height` and writes the bucketed data as partitioned parquet files.

Args:
  --outputPath - A directory to write the parquet files.

## Data Model

```sql
create table weather_balloon_data (
  id char(100),
  sounding_date integer,
  hour integer,
  release_time integer,
  number_of_levels integer,
  pressure_source_code char(50),
  non_pressure_source_code char(50),
  latitude integer,
  longitude integer,
  major_level_type integer,
  minor_level_type integer,
  elapsed_time_since_launch integer,
  pressure integer,
  pressure_flag  char(50),
  geo_potential_height integer,
  geo_potential_height_flag  char(50),
  temperature integer,
  temperature_processing_flag   char(50),
  relative_humidity  integer,
  dew_point_depression integer,
  wind_direction integer,
  wind_speed integer
)

CREATE INDEX weather_balloon_idx ON weather_balloon_data (sounding_date);
```

## How to Run

1. Run Tests

```shell
test (from sbt-shell) or sbt "test" (from terminal)
```

2. Build the project

```shell
clean;assembly (from sbt-shell) or sbt "clean;assembly" (from terminal)
```

3. Run the application

Load Weather data:
```shell
java -cp target\scala-2.12\adjust-data-challenge.jar com.adjust.data.WeatherDataLoader --inputPath "C:\Users\User\Downloads\USM0
0070219-data"
```
Process Weather data:
```shell
java -cp target\scala-2.12\adjust-data-challenge.jar com.adjust.data.WeatherDataProcessor --outputPath "C:\Users\User\Downloads\USM0
0070219-data-output"
```

sbt "runMain com.adjust.data.WeatherDataLoader --inputPath C:\Users\User\Downloads\USM00070219-data"