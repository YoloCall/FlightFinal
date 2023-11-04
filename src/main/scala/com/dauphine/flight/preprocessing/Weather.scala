package com.dauphine.flight.preprocessing

import com.dauphine.flight.conf.SparkSessionWrapper
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

object Weather extends SparkSessionWrapper {

  def preprocessWeather(data : DataFrame): DataFrame = {

    val windowSpec  = Window.partitionBy("WBAN").orderBy("DATETIME_RND")

    val df_weather_clean = data
      .select("WBAN", "Date", "Time", "SkyCondition", "Visibility", "DryBulbCelsius", "RelativeHumidity","WindSpeed","RecordType", "Altimeter")

      // convert date & time to datetime with rounded hours
      .withColumn("Date", unix_timestamp(from_unixtime(unix_timestamp(col("Date").cast("string"), "yyyyMMdd"))))
      .withColumn("DATETIME_RND", col("Date") + col("Time") * 60)
      .withColumn("DATETIME_RND", to_timestamp(from_unixtime(col("DATETIME_RND"), "yyyy-MM-dd HH:mm:ss")))
      .withColumn("hour", hour((round(unix_timestamp(col("DATETIME_RND")) / 3600) * 3600).cast("timestamp")))
      .withColumn("DATETIME_RND", unix_timestamp(to_date(col("DATETIME_RND"),"yyyy-MM-dd")))
      .withColumn("DATETIME_RND", col("DATETIME_RND") + col("hour") * 60 * 60)
      .withColumn("DATETIME_RND", to_timestamp(from_unixtime(col("DATETIME_RND"), "yyyy-MM-dd HH:mm:ss")))
      .withColumn("RecordType",when(col("RecordType")=== "SP","1").otherwise("0").cast("int"))
      .drop(col("hour"))

      //valeurs par défaut pour les features basées sur des médianes et classe de retard moyen
      .withColumn("SkyCondition",when(col("SkyCondition").isNull,"FEW060").otherwise(col("SkyCondition")))
      .withColumn("Visibility",when(col("Visibility").isNull,10).otherwise(col("Visibility")))
      .withColumn("DryBulbCelsius",when(col("DryBulbCelsius").isNull,17).otherwise(col("DryBulbCelsius")))
      .withColumn("RelativeHumidity",when(col("RelativeHumidity").isNull,72).otherwise(col("RelativeHumidity")))
      .withColumn("WindSpeed",when(col("WindSpeed").isNull,6).otherwise(col("WindSpeed")))
      .withColumn("RecordType",when(col("RecordType").isNull,0).otherwise(col("RecordType")))
      .withColumn("Altimeter",round(when(col("Altimeter").isNull,30).otherwise(col("Altimeter")),2))

      // Extraction des données de Sky
      .withColumn("Sky1",split(col("SkyCondition")," ").getItem(0))
      .withColumn("Sky1_type",substring(col("Sky1"),1,3))
      .withColumn("Sky1_height",when(
        substring(col("Sky1"),4,3).cast("int").isNull,60
      ).otherwise(
        substring(col("Sky1"),4,3)
      ).cast("int"))
      .withColumn("clearSky", when(col("Sky1_type") === "CLR", 1).otherwise(0))
      .withColumn("fewClouds", when(col("Sky1_type") === "FEW", 1).otherwise(0))
      .withColumn("scatterClouds", when(col("Sky1_type") === "SCT", 1).otherwise(0))
      .withColumn("brokenClouds", when(col("Sky1_type") === "BKN", 1).otherwise(0))
      .withColumn("overCast", when(col("Sky1_type") === "OVC", 1).otherwise(0))
      .withColumn("obscuredSky", when(col("Sky1_type") === "VV0" || col("Sky1_type") === "VV1", 1).otherwise(0))

      .select("WBAN","DATETIME_RND","Sky1_height", "Visibility", "DryBulbCelsius", "RelativeHumidity","WindSpeed","RecordType", "Altimeter","clearSky","fewClouds","scatterClouds","brokenClouds","overCast","obscuredSky")

      // drop useless, duplicates, & nulls
      .dropDuplicates(Seq("WBAN", "DATETIME_RND"))

      // create lags
      .withColumn("Sky1_height1", lag("Sky1_height", 1).over(windowSpec))
      .withColumn("Visibility1", lag("Visibility", 1).over(windowSpec))
      .withColumn("DryBulbCelsius1", lag("DryBulbCelsius", 1).over(windowSpec))
      .withColumn("RelativeHumidity1", lag("RelativeHumidity", 1).over(windowSpec))
      .withColumn("WindSpeed1", lag("WindSpeed", 1).over(windowSpec))
      .withColumn("RecordType1", lag("RecordType", 1).over(windowSpec))
      .withColumn("Altimeter1", lag("Altimeter", 1).over(windowSpec))
      .withColumn("clearSky1", lag("clearSky", 1).over(windowSpec))
      .withColumn("fewClouds1", lag("fewClouds", 1).over(windowSpec))
      .withColumn("scatterClouds1", lag("scatterClouds", 1).over(windowSpec))
      .withColumn("brokenClouds1", lag("brokenClouds", 1).over(windowSpec))
      .withColumn("overCast1", lag("overCast", 1).over(windowSpec))
      .withColumn("obscuredSky1", lag("obscuredSky", 1).over(windowSpec))

      .withColumn("Sky1_height2", lag("Sky1_height1", 1).over(windowSpec))
      .withColumn("Visibility2", lag("Visibility1", 1).over(windowSpec))
      .withColumn("DryBulbCelsius2", lag("DryBulbCelsius1", 1).over(windowSpec))
      .withColumn("RelativeHumidity2", lag("RelativeHumidity1", 1).over(windowSpec))
      .withColumn("WindSpeed2", lag("WindSpeed1", 1).over(windowSpec))
      .withColumn("RecordType2", lag("RecordType1", 1).over(windowSpec))
      .withColumn("Altimeter2", lag("Altimeter1", 1).over(windowSpec))
      .withColumn("clearSky2", lag("clearSky1", 1).over(windowSpec))
      .withColumn("fewClouds2", lag("fewClouds1", 1).over(windowSpec))
      .withColumn("scatterClouds2", lag("scatterClouds1", 1).over(windowSpec))
      .withColumn("brokenClouds2", lag("brokenClouds1", 1).over(windowSpec))
      .withColumn("overCast2", lag("overCast1", 1).over(windowSpec))
      .withColumn("obscuredSky2", lag("obscuredSky1", 1).over(windowSpec))

      .withColumn("Sky1_height3", lag("Sky1_height2", 1).over(windowSpec))
      .withColumn("Visibility3", lag("Visibility2", 1).over(windowSpec))
      .withColumn("DryBulbCelsius3", lag("DryBulbCelsius2", 1).over(windowSpec))
      .withColumn("WindSpeed3", lag("WindSpeed2", 1).over(windowSpec))
      .withColumn("RelativeHumidity3", lag("RelativeHumidity2", 1).over(windowSpec))
      .withColumn("RecordType3", lag("RecordType2", 1).over(windowSpec))
      .withColumn("Altimeter3", lag("Altimeter2", 1).over(windowSpec))
      .withColumn("clearSky3", lag("clearSky2", 1).over(windowSpec))
      .withColumn("fewClouds3", lag("fewClouds2", 1).over(windowSpec))
      .withColumn("scatterClouds3", lag("scatterClouds2", 1).over(windowSpec))
      .withColumn("brokenClouds3", lag("brokenClouds2", 1).over(windowSpec))
      .withColumn("overCast3", lag("overCast2", 1).over(windowSpec))
      .withColumn("obscuredSky3", lag("obscuredSky2", 1).over(windowSpec))

      //on rajoute une colonne pour tager les lignes avec une météo parfaitement calme; cela permettra d'écarter un certain nombre de retards non liés à la météo
      .withColumn("CalmWeather",when(
        col("Sky1_height") > 25 and
        col("Visibility") > 4 and not(
          col("DryBulbCelsius").between(-9,-2)) and
        col("RelativeHumidity") < 90 and
        col("WindSpeed") < 25 and
        col("RecordType") =!= 1 and
        col("obscuredSky") =!= 1 and
        col("Sky1_height1") > 25 and
        col("Visibility1") > 4 and not(
          col("DryBulbCelsius1").between(-9,-2)) and
        col("RelativeHumidity1") < 90 and
        col("WindSpeed1") < 25 and
        col("RecordType1") =!= 1 and
        col("obscuredSky1") =!= 1 and
        col("Sky1_height2") > 25 and
        col("Visibility2") > 4 and not(
          col("DryBulbCelsius2").between(-9,-2)) and
        col("RelativeHumidity2") < 90 and
        col("WindSpeed2") < 25 and
        col("RecordType2") =!= 1 and
        col("obscuredSky2") =!= 1 and
        col("Sky1_height3") > 25 and
        col("Visibility3") > 4 and not(
          col("DryBulbCelsius3").between(-9,-2)) and
        col("RelativeHumidity3") < 90 and
        col("WindSpeed3") < 25 and
        col("RecordType3") =!= 1 and
        col("obscuredSky3") =!= 1,1.0
      ).otherwise(0.0))

      // drop les rows avec plus de 7 feat nulls car le lag sur la première ligne donne des null : 3 * 7 features
      .na.drop(7)


    df_weather_clean
  }

}