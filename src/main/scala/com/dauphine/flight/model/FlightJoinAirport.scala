package com.dauphine.flight.model

import org.apache.spark.sql.types.{IntegerType, StructField, StructType, TimestampType}

object FlightJoinAirport {

  val flightJoinAirportSchema = StructType(Array(
    StructField("DEP_AIRPORT_ID", IntegerType, true),
    StructField("ARR_AIRPORT_ID", IntegerType, true),
    StructField("ARR_DELAY_NEW", IntegerType, true),
    StructField("CRS_ELAPSED_TIME", IntegerType, true),
    StructField("WEATHER_DELAY", IntegerType, true),
    StructField("ARR_hour", IntegerType, true),
    StructField("NAS_DELAY", IntegerType, true),
    StructField("WBAN_DEP", IntegerType, true),
    StructField("WBAN_ARR", IntegerType, true),
    StructField("TimeZone_DEP", IntegerType, true),
    StructField("TimeZone_ARR", IntegerType, true),
    StructField("LOCAL_DEP_DT", TimestampType, true),
    StructField("LOCAL_DEP_DT_RND", TimestampType, true),
    StructField("LOCAL_ARR_DT", TimestampType, true),
    StructField("LOCAL_ARR_DT_RND", TimestampType, true),
    StructField("IS_DELAYED", IntegerType, true),
  ))

}
