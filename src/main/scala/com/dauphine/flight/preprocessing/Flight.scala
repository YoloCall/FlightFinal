package com.dauphine.flight.preprocessing

import com.dauphine.flight.conf.SparkSessionWrapper
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object Flight extends SparkSessionWrapper {

  def preprocessFlight(data : DataFrame): DataFrame = {

    val delayThreshold = 30

    val df_flights_clean = data
      .drop("_c12") // 478k
      .where(col("CANCELLED") === 0).drop("CANCELLED")
      .where(col("DIVERTED") === 0).drop("DIVERTED") // 478k
      .dropDuplicates(Seq("FL_DATE", "OP_CARRIER_AIRLINE_ID", "OP_CARRIER_FL_NUM", "ORIGIN_AIRPORT_ID")) // 437k
      .withColumnRenamed("ORIGIN_AIRPORT_ID", "DEP_AIRPORT_ID")
      .withColumnRenamed("DEST_AIRPORT_ID", "ARR_AIRPORT_ID")

      // get Flight date time with hours & minutes
      .withColumn("PAD_CRS_DEP_TIME", lpad(col("CRS_DEP_TIME"), 4, "0"))
      .withColumn("FL_DATETIME",
        to_timestamp(
          concat(
            split(col("FL_DATE")," ").getItem(0),
            lit(' '),
            col("PAD_CRS_DEP_TIME")
          ),"yyyy-MM-dd HHmm"))
      .drop("PAD_CRS_DEP_TIME")

      // change other types
      .na.fill(0, Seq("NAS_DELAY", "WEATHER_DELAY"))
      .withColumn("NAS_DELAY", col("NAS_DELAY").cast("int"))
      .withColumn("WEATHER_DELAY", col("WEATHER_DELAY").cast("int"))
      .withColumn("CRS_ELAPSED_TIME", col("CRS_ELAPSED_TIME").cast("int"))
      .withColumn("ARR_DELAY_NEW", col("ARR_DELAY_NEW").cast("int"))

      // filter inconsistent values
      .where(!(col("ARR_DELAY_NEW") < -45))
      .where(col("ARR_DELAY_NEW") < (5.3 * 60))

      // create target
      .withColumn("IS_DELAYED", when(col("ARR_DELAY_NEW") >= delayThreshold and (col("NAS_DELAY") >= delayThreshold or col("WEATHER_DELAY") > 0), 1).otherwise(0))


    df_flights_clean

  }

  def getFlightJoinAirport (data_flight: DataFrame, data_airport: DataFrame) : DataFrame = {

    val df_flight_join_weather = data_flight
      .join(data_airport, data_airport("AirportID") === data_flight("DEP_AIRPORT_ID"), "inner")
      .withColumnRenamed("WBAN", "WBAN_ARR")
      .withColumnRenamed("TimeZone", "TimeZone_ARR")
      .drop("AirportID")
      .join(data_airport, data_airport("AirportID") === data_flight("ARR_AIRPORT_ID"), "inner")
      .withColumnRenamed("WBAN", "WBAN_DEP")
      .withColumnRenamed("TimeZone", "TimeZone_DEP")
      .drop("AirportID")

      // round departure date time to the nearest hour
      .withColumnRenamed("FL_DATETIME", "LOCAL_DEP_DT")
      .withColumn("DEP_hour", hour((round(unix_timestamp(col("LOCAL_DEP_DT")) / 3600) * 3600).cast("timestamp")))
      .withColumn("LOCAL_DEP_DT_RND", unix_timestamp(to_date(col("LOCAL_DEP_DT"),"yyyy-MM-dd")))
      .withColumn("LOCAL_DEP_DT_RND", col("LOCAL_DEP_DT_RND") + col("DEP_hour") * 60 * 60)
      .withColumn("LOCAL_DEP_DT_RND", to_timestamp(from_unixtime(col("LOCAL_DEP_DT_RND"), "yyyy-MM-dd HH:mm:ss")))
      .drop(col("DEP_hour"))

      // compute LOCAL arrival datetime
      .withColumn("DEP_DT", col("LOCAL_DEP_DT"))
      .withColumn("DEP_DT", unix_timestamp(date_format(col("DEP_DT"), "yyyy-MM-dd HH:mm:ss")))
      .withColumn("LOCAL_ARR_DT", col("DEP_DT") + col("CRS_ELAPSED_TIME") * 60  + col("ARR_DELAY_NEW") * 60 + (col("TimeZone_ARR") - col("TimeZone_DEP")) * 60 * 60)
      .withColumn("LOCAL_ARR_DT", to_timestamp(from_unixtime(col("LOCAL_ARR_DT"), "yyyy-MM-dd HH:mm:ss")))
      .drop(col("DEP_DT"))

      // round to the hour
      .withColumn("ARR_hour", hour((round(unix_timestamp(col("LOCAL_ARR_DT")) / 3600) * 3600).cast("timestamp")))
      .withColumn("LOCAL_ARR_DT_RND", unix_timestamp(to_date(col("LOCAL_ARR_DT"),"yyyy-MM-dd HH:mm:ss")))
      .withColumn("LOCAL_ARR_DT_RND", col("LOCAL_ARR_DT_RND") + col("ARR_hour") * 60 * 60)
      .withColumn("LOCAL_ARR_DT_RND", to_timestamp(from_unixtime(col("LOCAL_ARR_DT_RND"), "yyyy-MM-dd HH:mm:ss")))
      .withColumn("LOCAL_ARR_DT_RND", (col("LOCAL_ARR_DT_RND")))

    df_flight_join_weather

  }
}
