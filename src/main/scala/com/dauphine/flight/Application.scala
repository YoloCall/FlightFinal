package com.dauphine.flight

import java.util.logging.Logger

import com.dauphine.flight.conf.{Constant, SparkSessionWrapper}
import com.dauphine.flight.ml.PredictDelay
import com.dauphine.flight.model.{FlightJoinAirport, GoldSchema, WeatherSchema}
import com.dauphine.flight.preprocessing.{Flight, Gold, Weather}
import com.dauphine.flight.services.HadoopService


object Application extends SparkSessionWrapper {

  val logger: Logger = Logger.getLogger(this.getClass.getName)

  def main(args: Array[String]): Unit = {

    val t0 = System.nanoTime()

    logger.info("Given arguments: " + args.mkString(" "))
    val typeServeur = args(args.indexOf("-s") + 1)

    logger.info("Get data")
    val df_flights_input = HadoopService.getDataFromHdfs(Constant.hdfsFlightRemotePath).cache()
    val df_airports_input = HadoopService.getDataCsvFromHdfs(Constant.hdfsAirportRemotePath).cache()
    val df_weather_input = HadoopService.getDataCsvFromHdfs(Constant.hdfsWeatherRemotePath).cache()

    logger.info("Preprocess data")
    val df_flights_preprocess = Flight.preprocessFlight(df_flights_input)
    val df_flight_airport = Flight.getFlightJoinAirport(df_flights_preprocess, df_airports_input)
    val df_weather = Weather.preprocessWeather(df_weather_input)

    logger.info("Write to hdfs")
    HadoopService.writeOrcToHdfs(df_flights_preprocess, Constant.hdfsFlightRemoteOuput)
    HadoopService.writeOrcToHdfs(df_weather, Constant.hdfsWeatherRemoteOuput)
    HadoopService.writeOrcToHdfs(df_airports_input, Constant.hdfsAirportRemoteOuput)
    HadoopService.writeOrcToHdfs(df_flight_airport, Constant.hdfsFlightSilverRemoteOuput)

    logger.info("Get silver data")
    val df_flights_silver = HadoopService.getOrcWithSchemaFromHdfs(FlightJoinAirport.flightJoinAirportSchema, Constant.hdfsFlightSilverRemoteOuput ).cache()
    val df_weather_silver = HadoopService.getOrcWithSchemaFromHdfs(WeatherSchema.weatherSchema, Constant.hdfsWeatherRemoteOuput).cache()

    logger.info("Preprocess gold")
    val df_gold = Gold.preprocessGold(df_flights_silver, df_weather_silver)

    logger.info("Write gold to hdfs")
    HadoopService.writeOrcToHdfs(df_gold, Constant.hdfsGoldRemoteOuput)

    logger.info("Get gold with schema")
    val df_gold_final = HadoopService.getOrcWithSchemaFromHdfs(GoldSchema.goldSchema, Constant.hdfsGoldRemoteOuput)
      .drop("LOCAL_DEP_DT", "LOCAL_ARR_DT", "DEP_DATETIME_RND", "ARR_DATETIME_RND")
      .cache()

    logger.info("Make prediction")
    PredictDelay.getRandomForest(df_gold_final)

    val t1 = System.nanoTime()
    println("Elapsed time: " + (t1 - t0) + "ns")

  }

}
