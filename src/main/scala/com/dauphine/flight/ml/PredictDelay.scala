package com.dauphine.flight.ml

import com.dauphine.flight.conf.{Constant, SparkSessionWrapper}
import com.dauphine.flight.services.HadoopService
import org.apache.spark.ml.classification.{DecisionTreeClassifier, RandomForestClassifier}
import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object PredictDelay extends SparkSessionWrapper {

  def getDecisionTree(data: DataFrame) : Unit = {

    // columns that need to added to feature column
    val featColumns = data.columns.filter(name => !name.contains("IS_DELAYED"))

    // VectorAssembler to add feature column
    // input columns - cols
    // feature column - features
    val assembler = new VectorAssembler()
      .setInputCols(featColumns)
      .setOutputCol("features")
      .setHandleInvalid("skip") // options are "keep", "error" or "skip"

    val featureDf = assembler.transform(data)

    // StringIndexer define new 'label' column with 'result' column
    // Index labels, adding metadata to the label column.
    // Fit on whole dataset to include all labels in index.
    val labelIndexer = new StringIndexer()
      .setInputCol("IS_DELAYED")
      .setOutputCol("label")


    val labelDf = labelIndexer.fit(featureDf).transform(featureDf)

    // Split the data into training and test sets (30% held out for testing).
    // split data set training and test
    // training data set - 70%
    // test data set - 30%
    val seed = 42

    val zeros = labelDf.filter(col("IS_DELAYED") === 0).sample(0.1, seed) // on ne prend que 10% des flights qui sont à l'heure
    val ones = labelDf.filter(col("IS_DELAYED") === 1)

    val Array(train0, test0) = zeros.randomSplit(Array(0.7, 0.3), seed)
    val Array(train1, test1) = ones.randomSplit(Array(0.7, 0.3), seed)

    val trainingData = train0.union(train1)
    val testData = test0.union(test1)

    val decisionTreeClassifier = new DecisionTreeClassifier()
      .setImpurity("gini")
      .setMaxDepth(20)
      .setSeed(seed)
      .setLabelCol("label")
      .setFeaturesCol("features")

    val decisionTreeModel = decisionTreeClassifier.fit(trainingData)

    val predictionDf = decisionTreeModel.transform(testData)

    val predictionAndLabels = predictionDf
      .select(col("prediction").cast("double"), col("label").cast("double"))

    HadoopService.writeOrcToHdfs(predictionDf, Constant.hdfsPredicRemoteOuput)

    val truePositif = predictionDf.filter(col("prediction") === 1 && col("label") === col("prediction")).count()
    val trueNegatif = predictionDf.filter(col("prediction") === 0 && col("label") === col("prediction")).count()
    val falsePositif = predictionDf.filter(col("prediction") === 1 && col("label") =!= col("prediction")).count()
    val falseNegatif = predictionDf.filter(col("prediction") === 0 && col("label") =!= col("prediction")).count()


    val precision = truePositif / (truePositif + falsePositif)
    val recall = truePositif / (truePositif + falseNegatif)
    val acc = (truePositif + trueNegatif) / (truePositif + trueNegatif + falsePositif + falseNegatif)
    val f1score = 2 * precision * recall / (precision + recall)

    println(s"Test Error = ${(1.0 - acc)}, Test precision = ${precision}, Test recall = ${recall}, Test f1score = ${f1score}, truePositif = ${truePositif}, trueNegatif = ${trueNegatif}, falsePositif = ${falsePositif}, falseNegatif = ${falseNegatif},")

  }

  def getRandomForest (data: DataFrame) : Unit = {

    // columns that need to added to feature column
    val featColumns = data.columns.filter(name => !name.contains("IS_DELAYED"))

    // VectorAssembler to add feature column
    // input columns - cols
    // feature column - features
    val vectorAssembler = new VectorAssembler()
      .setInputCols(featColumns)
      .setOutputCol("features")
      .setHandleInvalid("skip") // options are "keep", "error" or "skip"


    val featureDf = vectorAssembler.transform(data)

    // StringIndexer define new 'label' column with 'result' column
    // Index labels, adding metadata to the label column.
    // Fit on whole dataset to include all labels in index.
    val labelIndexer = new StringIndexer()
      .setInputCol("IS_DELAYED")
      .setOutputCol("label")


    val labelDf = labelIndexer.fit(featureDf).transform(featureDf)

    // Split the data into training and test sets (30% held out for testing).
    // split data set training and test
    // training data set - 70%
    // test data set - 30%
    val seed = 42

    val zeros = labelDf.filter(col("IS_DELAYED") === 0).sample(0.1, seed) // on ne prend que 10% des flights qui sont à l'heure
    val ones = labelDf.filter(col("IS_DELAYED") === 1)

    val Array(train0, test0) = zeros.randomSplit(Array(0.7, 0.3), seed)
    val Array(train1, test1) = ones.randomSplit(Array(0.7, 0.3), seed)

    val trainingData = train0.union(train1)
    val testData = test0.union(test1)

    println(f"""There are ${trainingData.count} rows in the training set, and ${testData.count} in the test set""")

    // train Random Forest model with training data set
    val randomForestClassifier = new RandomForestClassifier()
      .setImpurity("gini")
      .setMaxDepth(15)
      .setNumTrees(30)
      .setFeatureSubsetStrategy("auto")
      .setSeed(seed)
      .setLabelCol("label") // indexedLabel
      .setFeaturesCol("features") // indexedFeatures

    val randomForestModel = randomForestClassifier.fit(trainingData)

    val predictionDf = randomForestModel.transform(testData)

    HadoopService.writeOrcToHdfs(predictionDf, Constant.hdfsPredicRemoteOuput)

    val truePositif = predictionDf.filter(col("prediction") === 1 && col("label") === col("prediction")).count()
    val trueNegatif = predictionDf.filter(col("prediction") === 0 && col("label") === col("prediction")).count()
    val falsePositif = predictionDf.filter(col("prediction") === 1 && col("label") =!= col("prediction")).count()
    val falseNegatif = predictionDf.filter(col("prediction") === 0 && col("label") =!= col("prediction")).count()

    val precision = truePositif / (truePositif + falsePositif)
    val recall = truePositif / (truePositif + falseNegatif)
    val acc = (truePositif + trueNegatif) / (truePositif + trueNegatif + falsePositif + falseNegatif)
    val f1score = 2 * precision * recall / (precision + recall)

    println(s"Test Error = ${(1.0 - acc)}, Test precision = ${precision}, Test recall = ${recall}, Test f1score = ${f1score}, truePositif = ${truePositif}, trueNegatif = ${trueNegatif}, falsePositif = ${falsePositif}, falseNegatif = ${falseNegatif},")


  }

}
