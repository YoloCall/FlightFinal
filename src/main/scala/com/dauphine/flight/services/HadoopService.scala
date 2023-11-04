package com.dauphine.flight.services

import com.dauphine.flight.conf.SparkSessionWrapper
import com.dauphine.flight.model.{FlightPreprocessSchema, WeatherSchema}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StructType

object HadoopService extends SparkSessionWrapper {

  def getDataFromHdfs(path: String): DataFrame = {

    val df_with_schema = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(path)

    df_with_schema

  }

  def getDataCsvFromHdfs(path: String): DataFrame = {

    val data = spark.read.format("csv")
      .option("header", "true")
      .option("delimited", ",")
      .option("treatEmptyValuesAsNulls","true")
      .option("nullValue", null)
      .option("emptyValue", null)
      .load(path)

    data

  }

  def writeCsvToHdfs(data: DataFrame, path: String): Unit = {

    data.write.option("header","true").mode("overwrite").csv(path)

  }

  def writeOrcToHdfs(data: DataFrame, path: String): Unit = {

    data.write.format("orc").mode("overwrite").save(path)

  }

  def getOrcDataFromHdfs(path: String): DataFrame = {

    val df_silver = spark.read
      .orc(path)

    df_silver

  }

  def getOrcWithSchemaFromHdfs(schema : StructType, path: String): DataFrame = {

    val df_silver = spark.read
      .schema(schema)
      .orc(path)

    df_silver

  }



}