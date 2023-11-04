package com.dauphine.flight.ml

import com.dauphine.flight.conf.SparkSessionWrapperTest
import com.dauphine.flight.services.HadoopService
import org.junit.jupiter.api.Test
import org.apache.spark.sql.functions._

class PredictDelayTest extends SparkSessionWrapperTest {

  @Test
  def testPrediction(): Unit = {

    val goldPath = "/Users/mathisperez/DevApp/ProjetFlight/src/test/resources/gold/ouput"

    val df_gold = HadoopService.getOrcDataFromHdfs(goldPath).cache()

    //val cols = Seq("FL_DATE", "LOCAL_DEP_DT","LOCAL_ARR_DT","ARR_DATETIME_RND","DEP_WindSpeed","DEP_WindSpeed1","DEP_WindSpeed2","DEP_WindSpeed3","ARR_WindSpeed","ARR_WindSpeed1","ARR_WindSpeed2","ARR_WindSpeed3")

    //val df_gold_clean = df_gold.drop(cols:_*)

    //df_gold_clean.show(false)

    PredictDelay.getDecisionTree(df_gold)
    //val df_prediction = PredictDelay.getDecisionTree(df_gold_clean)
    //df_prediction.show(false)


  }

}
