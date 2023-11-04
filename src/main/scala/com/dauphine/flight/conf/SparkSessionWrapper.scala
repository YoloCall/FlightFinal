package com.dauphine.flight.conf

import java.io.File
import org.apache.spark.sql.SparkSession

trait SparkSessionWrapper {

  val warehouseLocation = new File("spark-warehouse").getAbsolutePath

  lazy val spark : SparkSession = SparkSession.builder()
    //.master("local[*]")
    //.master("spark://macbook-pro-de-mathis.home:7077")
    .config("spark.submit.deployMode","cluster")
    //.config("spark.sql.warehouse.dir", "hdfs://127.0.0.1:9000/user/hive/warehouse/")
    .enableHiveSupport()
    .appName("Flight")
    .getOrCreate()

}
