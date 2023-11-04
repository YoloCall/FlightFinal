package com.dauphine.flight.preprocessing

import com.dauphine.flight.conf.SparkSessionWrapperTest
import com.dauphine.flight.model.{FlightJoinAirport, FlightPreprocessSchema, WeatherSchema}
import com.dauphine.flight.services.HadoopService
import org.junit.jupiter.api.Test

class FlightTest extends SparkSessionWrapperTest {

  @Test
  def testPreprocessFlight(): Unit = {

    val flightPath = "src/test/resources/flight/201205.csv"

    val df_flight_input = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(flightPath)

    val df_flight_preprocess = Flight.preprocessFlight(df_flight_input).cache()

    HadoopService.writeOrcToHdfs(df_flight_preprocess, "src/test/resources/flight/outputPreprocess")

    val df_flight_preprocess_silver = HadoopService.getOrcWithSchemaFromHdfs(FlightPreprocessSchema.flightSchema,"src/test/resources/flight/outputPreprocess").cache()

    df_flight_preprocess_silver.show(5,false)
    df_flight_preprocess_silver.printSchema()

  }

  @Test
  def testFlightJoinAirport(): Unit = {

    // Get path
    val airportPath = "src/test/resources/airport/wban_airport_timezone.csv"
    val flightPreprocessPath = "src/test/resources/flight/outputPreprocess"
    val flightSilverPath = "/Users/mathisperez/DevApp/ProjetFlight/src/test/resources/flight/outputSilver"

    // Get data
    val df_airport = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(airportPath)
    val df_flight_preprocess = HadoopService.getOrcWithSchemaFromHdfs(FlightPreprocessSchema.flightSchema, flightPreprocessPath).cache()

    // Join flight to airport and write to hdfs
    val df_flight_airport = Flight.getFlightJoinAirport(df_flight_preprocess, df_airport).cache()
    HadoopService.writeOrcToHdfs(df_flight_airport, flightSilverPath)

    // Get flight_silver
    val df_flight_silver = HadoopService.getOrcWithSchemaFromHdfs(FlightJoinAirport.flightJoinAirportSchema, flightSilverPath).cache()

    df_flight_silver.show(5,false)
    df_flight_silver.printSchema()

  }

}
