package com.dauphine.flight.preprocessing

import com.dauphine.flight.conf.SparkSessionWrapper
import org.apache.spark.sql.DataFrame

object Gold extends SparkSessionWrapper {

  def preprocessGold(dataFlight : DataFrame, dataWeather : DataFrame): DataFrame = {

    val finalDropList = List("LOCAL_DEP_DT_RND", "LOCAL_ARR_DT_RND", "DEP_WBAN")


    val df_final = dataFlight
      // join the weather dataset 2 times
      .join(dataWeather, dataWeather("WBAN") === dataFlight("WBAN_DEP") && dataWeather("DATETIME_RND") === dataFlight("LOCAL_DEP_DT_RND"), "inner")

      // rename all weather columns
      .withColumnRenamed("WBAN", "DEP_WBAN")
      .withColumnRenamed("DATETIME_RND", "DEP_DATETIME_RND")
      .withColumnRenamed("CalmWeather", "DEP_CalmWeather")

      .withColumnRenamed("Sky1_height", "DEP_Sky1_height")
      .withColumnRenamed("Visibility", "DEP_Visibility")
      .withColumnRenamed("DryBulbCelsius", "DEP_DryBulbCelsius")
      .withColumnRenamed("RelativeHumidity", "DEP_RelativeHumidity")
      .withColumnRenamed("WindSpeed", "DEP_WindSpeed")
      .withColumnRenamed("RecordType", "DEP_RecordType")
      .withColumnRenamed("Altimeter", "DEP_Altimeter")
      .withColumnRenamed("clearSky", "DEP_clearSky")
      .withColumnRenamed("fewClouds", "DEP_fewClouds")
      .withColumnRenamed("scatterClouds", "DEP_scatterClouds")
      .withColumnRenamed("brokenClouds", "DEP_brokenClouds")
      .withColumnRenamed("overCast", "DEP_overCast")
      .withColumnRenamed("obscuredSky", "DEP_obscuredSky")

      .withColumnRenamed("Sky1_height1", "DEP_Sky1_height1")
      .withColumnRenamed("Visibility1", "DEP_Visibility1")
      .withColumnRenamed("DryBulbCelsius1", "DEP_DryBulbCelsius1")
      .withColumnRenamed("RelativeHumidity1", "DEP_RelativeHumidity1")
      .withColumnRenamed("WindSpeed1", "DEP_WindSpeed1")
      .withColumnRenamed("RecordType1", "DEP_RecordType1")
      .withColumnRenamed("Altimeter1", "DEP_Altimeter1")
      .withColumnRenamed("clearSky1", "DEP_clearSky1")
      .withColumnRenamed("fewClouds1", "DEP_fewClouds1")
      .withColumnRenamed("scatterClouds1", "DEP_scatterClouds1")
      .withColumnRenamed("brokenClouds1", "DEP_brokenClouds1")
      .withColumnRenamed("overCast1", "DEP_overCast1")
      .withColumnRenamed("obscuredSky1", "DEP_obscuredSky1")

      .withColumnRenamed("Sky1_height2", "DEP_Sky1_height2")
      .withColumnRenamed("Visibility2", "DEP_Visibility2")
      .withColumnRenamed("DryBulbCelsius2", "DEP_DryBulbCelsius2")
      .withColumnRenamed("RelativeHumidity2", "DEP_RelativeHumidity2")
      .withColumnRenamed("WindSpeed2", "DEP_WindSpeed2")
      .withColumnRenamed("RecordType2", "DEP_RecordType2")
      .withColumnRenamed("Altimeter2", "DEP_Altimeter2")
      .withColumnRenamed("clearSky2", "DEP_clearSky2")
      .withColumnRenamed("fewClouds2", "DEP_fewClouds2")
      .withColumnRenamed("scatterClouds2", "DEP_scatterClouds2")
      .withColumnRenamed("brokenClouds2", "DEP_brokenClouds2")
      .withColumnRenamed("overCast2", "DEP_overCast2")
      .withColumnRenamed("obscuredSky2", "DEP_obscuredSky2")

      .withColumnRenamed("Sky1_height3", "DEP_Sky1_height3")
      .withColumnRenamed("Visibility3", "DEP_Visibility3")
      .withColumnRenamed("DryBulbCelsius3", "DEP_DryBulbCelsius3")
      .withColumnRenamed("RelativeHumidity3", "DEP_RelativeHumidity3")
      .withColumnRenamed("WindSpeed3", "DEP_WindSpeed3")
      .withColumnRenamed("RecordType3", "DEP_RecordType3")
      .withColumnRenamed("Altimeter3", "DEP_Altimeter3")
      .withColumnRenamed("clearSky3", "DEP_clearSky3")
      .withColumnRenamed("fewClouds3", "DEP_fewClouds3")
      .withColumnRenamed("scatterClouds3", "DEP_scatterClouds3")
      .withColumnRenamed("brokenClouds3", "DEP_brokenClouds3")
      .withColumnRenamed("overCast3", "DEP_overCast3")
      .withColumnRenamed("obscuredSky3", "DEP_obscuredSky3")

      // repartition
      //.repartition(col("WBAN_ARR"), col("LOCAL_ARR_DT_RND"))

      // join the weather dataset 2 times
      .join(dataWeather, dataWeather("WBAN") === dataFlight("WBAN_ARR") && dataWeather("DATETIME_RND") === dataFlight("LOCAL_ARR_DT_RND"), "inner")

      // rename all weather columns
      .withColumnRenamed("WBAN", "ARR_WBAN")
      .withColumnRenamed("DATETIME_RND", "ARR_DATETIME_RND")
      .withColumnRenamed("CalmWeather", "ARR_CalmWeather")

      .withColumnRenamed("Sky1_height", "ARR_Sky1_height")
      .withColumnRenamed("Visibility", "ARR_Visibility")
      .withColumnRenamed("DryBulbCelsius", "ARR_DryBulbCelsius")
      .withColumnRenamed("RelativeHumidity", "ARR_RelativeHumidity")
      .withColumnRenamed("WindSpeed", "ARR_WindSpeed")
      .withColumnRenamed("RecordType", "ARR_RecordType")
      .withColumnRenamed("Altimeter", "ARR_Altimeter")
      .withColumnRenamed("clearSky", "ARR_clearSky")
      .withColumnRenamed("fewClouds", "ARR_fewClouds")
      .withColumnRenamed("scatterClouds", "ARR_scatterClouds")
      .withColumnRenamed("brokenClouds", "ARR_brokenClouds")
      .withColumnRenamed("overCast", "ARR_overCast")
      .withColumnRenamed("obscuredSky", "ARR_obscuredSky")

      .withColumnRenamed("Sky1_height1", "ARR_Sky1_height1")
      .withColumnRenamed("Visibility1", "ARR_Visibility1")
      .withColumnRenamed("DryBulbCelsius1", "ARR_DryBulbCelsius1")
      .withColumnRenamed("RelativeHumidity1", "ARR_RelativeHumidity1")
      .withColumnRenamed("WindSpeed1", "ARR_WindSpeed1")
      .withColumnRenamed("RecordType1", "ARR_RecordType1")
      .withColumnRenamed("Altimeter1", "ARR_Altimeter1")
      .withColumnRenamed("clearSky1", "ARR_clearSky1")
      .withColumnRenamed("fewClouds1", "ARR_fewClouds1")
      .withColumnRenamed("scatterClouds1", "ARR_scatterClouds1")
      .withColumnRenamed("brokenClouds1", "ARR_brokenClouds1")
      .withColumnRenamed("overCast1", "ARR_overCast1")
      .withColumnRenamed("obscuredSky1", "ARR_obscuredSky1")

      .withColumnRenamed("Sky1_height2", "ARR_Sky1_height2")
      .withColumnRenamed("Visibility2", "ARR_Visibility2")
      .withColumnRenamed("DryBulbCelsius2", "ARR_DryBulbCelsius2")
      .withColumnRenamed("RelativeHumidity2", "ARR_RelativeHumidity2")
      .withColumnRenamed("WindSpeed2", "ARR_WindSpeed2")
      .withColumnRenamed("RecordType2", "ARR_RecordType2")
      .withColumnRenamed("Altimeter2", "ARR_Altimeter2")
      .withColumnRenamed("clearSky2", "ARR_clearSky2")
      .withColumnRenamed("fewClouds2", "ARR_fewClouds2")
      .withColumnRenamed("scatterClouds2", "ARR_scatterClouds2")
      .withColumnRenamed("brokenClouds2", "ARR_brokenClouds2")
      .withColumnRenamed("overCast2", "ARR_overCast2")
      .withColumnRenamed("obscuredSky2", "ARR_obscuredSky2")

      .withColumnRenamed("Sky1_height3", "ARR_Sky1_height3")
      .withColumnRenamed("Visibility3", "ARR_Visibility3")
      .withColumnRenamed("DryBulbCelsius3", "ARR_DryBulbCelsius3")
      .withColumnRenamed("RelativeHumidity3", "ARR_RelativeHumidity3")
      .withColumnRenamed("WindSpeed3", "ARR_WindSpeed3")
      .withColumnRenamed("RecordType3", "ARR_RecordType3")
      .withColumnRenamed("Altimeter3", "ARR_Altimeter3")
      .withColumnRenamed("clearSky3", "ARR_clearSky3")
      .withColumnRenamed("fewClouds3", "ARR_fewClouds3")
      .withColumnRenamed("scatterClouds3", "ARR_scatterClouds3")
      .withColumnRenamed("brokenClouds3", "ARR_brokenClouds3")
      .withColumnRenamed("overCast3", "ARR_overCast3")
      .withColumnRenamed("obscuredSky3", "ARR_obscuredSky3")

      // drop useless cols
      .drop(finalDropList : _*)


    df_final
  }

}
