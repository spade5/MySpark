package org.apache.spark.streaming.util

import ml.dmlc.xgboost4j.scala.{Booster, DMatrix, XGBoost}

private[streaming] class Regression(trainLimit: Int = 10) {
  // xgboost regression
  /* private var model = XGBoost.train(new DMatrix("data/agaricus.txt.train"), Map("eta" -> 0.1f,
    "max_depth" -> 2, "objective" -> "reg:linear").toMap, 10, null, null) */

  private var dataTable = Array[Float]()
  private var nRow = 0
  private var nCol = 0
  private var model: Booster = _

  def gatherData(row: Array[Float]): Unit = {
    dataTable ++= row
    nCol = row.length
    nRow += 1
    // model.predict(new DMatrix(row))
    if (nRow >= trainLimit) {
      train()
    }
  }

  private def train(): Unit = {
    model = XGBoost.train(new DMatrix(dataTable, nRow, nCol, 0),
      Map("eta" -> 0.1f, "max_depth" -> 2,
    "objective" -> "reg:linear").toMap, 10, null, null)
  }

  def ready: Boolean = model != null

  def predict(row: Array[Float]): Option[Float] = {
    if (model == null) {
      None
    } else {
      Some(model.predict(new DMatrix(row, 1, nCol, 0))(0)(0))
    }
  }
}
