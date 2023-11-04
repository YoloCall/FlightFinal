package com.dauphine.flight.conf

import org.apache.spark.sql.SparkSession

trait SparkSessionWrapperTest {

  lazy val spark: SparkSession = SparkSession.builder()
    .master("local[1]")
    .appName("PageRank")
    .getOrCreate()

}
