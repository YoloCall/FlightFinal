package com.dauphine.flight.model

import org.apache.spark.sql.types.{IntegerType, StructField, StructType, TimestampType}

object FlightPreprocessSchema {

  val flightSchema = StructType(Array(
    StructField("DEP_AIRPORT_ID", IntegerType, true),
    StructField("ARR_AIRPORT_ID", IntegerType, true),
    StructField("ARR_DELAY_NEW", IntegerType, true),
    StructField("CRS_ELAPSED_TIME", IntegerType, true),
    StructField("WEATHER_DELAY", IntegerType, true),
    StructField("NAS_DELAY", IntegerType, true),
    StructField("FL_DATETIME", TimestampType, true),
    StructField("IS_DELAYED", IntegerType, true),
  ))

}
