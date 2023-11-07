package com.dauphine.flight.ml

import com.dauphine.flight.conf.SparkSessionWrapperTest
import com.dauphine.flight.model.GoldSchema
import com.dauphine.flight.services.HadoopService
import org.junit.jupiter.api.Test

class PredictTest extends SparkSessionWrapperTest {

  @Test
  def testPrediction(): Unit = {

    // Permet de debug : org.apache.spark.SparkException: A master URL must be set in your configuration
    // Le fait de faire un spark.read doit init un sparkContext
    val airportPath = "src/test/resources/airport/wban_airport_timezone.csv"
    val df_flight_input = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(airportPath)

    val goldPath = "src/test/resources/gold/output"
    val df_gold = HadoopService.getOrcWithSchemaFromHdfs(GoldSchema.goldSchema, goldPath)
      .drop("LOCAL_DEP_DT", "LOCAL_ARR_DT", "DEP_DATETIME_RND", "ARR_DATETIME_RND")
      .cache()

    val df_gold_clean = df_gold.drop("LOCAL_DEP_DT", "LOCAL_ARR_DT", "DEP_DATETIME_RND", "ARR_DATETIME_RND").cache()

    df_gold_clean.show(false)

    PredictDelay.getDecisionTree(df_gold_clean)
    //PredictDelay.getRandomForest(df_gold_clean)

  }


}
