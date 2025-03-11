package org.apache.spark.streaming.util

import ml.dmlc.xgboost4j.scala.{Booster, DMatrix, XGBoost}

import org.apache.spark.SparkConf

private[streaming] class Regression(conf: SparkConf) {

  private val FEATURE_NAMES = Array("total", "keys", "total_len", "std_count",
    "std_len", "cpu_freq", "cpu_load", "memory_usage")

  private val modelPath = conf.get("spark.streaming.regression.modelPath", null)
  private val model: Booster = if (modelPath != null) {
    // scalastyle:off println
    println(s"Loading model from $modelPath")
    try {
      val booster = XGBoost.loadModel(modelPath)
      println(s"Loaded model from $modelPath")
      val features = Array(290715.0f, 6370.0f, 1352669.0f,
        5.273871f, 1183.0406f, 2.4214f, 2.21f, 8.53f)
      val res = booster.predict(new DMatrix(features, 1, 8, 0))
      println("XGBoost Test:" + res(0).mkString("Array(", ", ", ")"))
      booster
    } catch {
      case e: Throwable =>
        println(s"Failed to load model from $modelPath: ${e.getMessage}")
        null
    }

  } else {
    null
  }

  def ready: Boolean = model != null

  def predict(features: Array[Array[Float]]): Option[Array[Float]] = {
    if (model == null || features.isEmpty) {
      None
    } else {
      // 创建 DMatrix 对象，添加特征名称
      val flattenFeatures: Array[Float] = features.flatten
      val numRows = features.length
      val numCols = features(0).length
      val dMatrix = new DMatrix(flattenFeatures, numRows, numCols, 0)

      // dMatrix.setFeatureNames(FEATURE_NAMES)

      val predictions = model.predict(dMatrix)

      val results = predictions.map(_(0))
      Some(results)
    }
  }
}
