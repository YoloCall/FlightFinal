package com.dauphine.flight.preprocessing

import com.dauphine.flight.conf.SparkSessionWrapperTest
import com.dauphine.flight.model.{FlightJoinAirport, FlightPreprocessSchema, GoldSchema, WeatherSchema}
import com.dauphine.flight.services.HadoopService
import org.junit.jupiter.api.Test

class GoldTest extends SparkSessionWrapperTest {

  @Test
  def testGold(): Unit = {

    // Permet de debug : org.apache.spark.SparkException: A master URL must be set in your configuration
    // Le fait de faire un spark.read doit init un sparkContext
    val flightPath = "src/test/resources/flight/201205.csv"
    val df_flight_input = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(flightPath)

    // Get path
    val flightSilverPath = "/Users/mathisperez/DevApp/ProjetFlight/src/test/resources/flight/ouputSilver"
    val weatherPath = "/Users/mathisperez/DevApp/ProjetFlight/src/test/resources/weather/output"
    val goldPath = "/Users/mathisperez/DevApp/ProjetFlight/src/test/resources/gold/output"

    // Get data
    val df_weather = HadoopService.getOrcWithSchemaFromHdfs(WeatherSchema.weatherSchema, weatherPath).cache()
    val df_flight_silver = HadoopService.getOrcWithSchemaFromHdfs(FlightJoinAirport.flightJoinAirportSchema, flightSilverPath).cache()

    // Preprocess Gold
    val df_gold = Gold.preprocessGold(df_flight_silver, df_weather)

    df_gold.show(5,false)
    df_gold.printSchema()

    HadoopService.writeOrcToHdfs(df_gold, goldPath)

    val df_gold_final = HadoopService.getOrcWithSchemaFromHdfs(GoldSchema.goldSchema, goldPath).cache()

    df_gold_final.show(5,false)
    df_gold_final.printSchema()

  }

}
