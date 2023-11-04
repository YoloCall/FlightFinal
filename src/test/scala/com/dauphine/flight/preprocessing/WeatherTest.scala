package com.dauphine.flight.preprocessing

import com.dauphine.flight.conf.SparkSessionWrapperTest
import com.dauphine.flight.model.WeatherSchema
import com.dauphine.flight.services.HadoopService
import org.junit.jupiter.api.Test

class WeatherTest extends SparkSessionWrapperTest {

  @Test
  def testPreprocessWeather(): Unit = {
    val weatherPath = "src/test/resources/weather"

    val df_weather_input = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(weatherPath)

    df_weather_input.show(false)

    val df_weather = Weather.preprocessWeather(df_weather_input).cache()

    HadoopService.writeOrcToHdfs(df_weather, "src/test/resources/weather/output")

    val df_weather_silver = HadoopService.getOrcWithSchemaFromHdfs(WeatherSchema.weatherSchema,"src/test/resources/weather/output").cache()

    df_weather_silver.show(5,false)
    df_weather_silver.printSchema()

  }

}
