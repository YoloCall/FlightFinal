package com.dauphine.flight.conf

import java.io.File
import org.apache.spark.sql.SparkSession

trait SparkSessionWrapper {

  val warehouseLocation = new File("spark-warehouse").getAbsolutePath

  lazy val spark : SparkSession = SparkSession.builder()
    .config("spark.submit.deployMode","cluster")
    .enableHiveSupport()
    .appName("Flight")
    .getOrCreate()

}
