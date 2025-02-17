package org.apache.spark.rdd

import scala.reflect.ClassTag

// scalastyle:off println
class BlockStats[T: ClassTag](private val name: String = "") extends Serializable {
  private var totalNum = 0L
  private var totalLength = 0L
  private val keyCountLengthMap = scala.collection.mutable.HashMap.empty[String, (Int, Int)]

  def insert(item: T, n: Int = 1): Unit = {
    totalNum += n

    // if item is a tuple, we use the first element as key
    var key: String = ""
    item match {
      case product: Product =>
        key = product.productElement(0).toString
      case _ =>
        key = item.toString
    }
    // println("key:" + key)
    totalLength += key.length
    val (count, _) = keyCountLengthMap.getOrElse(key, (0, 0))
    keyCountLengthMap.put(key, (count + n, key.length))

    // print()
  }

  def merge(other: BlockStats[T]): Unit = {
    totalNum += other.totalNum
    totalLength += other.totalLength
    other.keyCountLengthMap.foreach { case (key, (count, length)) =>
      val (thisCount, _) = keyCountLengthMap.getOrElse(key, (0, 0))
      keyCountLengthMap.put(key, (thisCount + count, length))
    }

    print()
  }

  def calc(): Array[Double] = {
    val keyCountLengthList = keyCountLengthMap.toList
    val keyNum = keyCountLengthList.length
    val meanLength = if (totalNum > 0) totalLength.toDouble / totalNum.toDouble else 0
    val meanCount = if (keyNum > 0) totalNum.toDouble / keyNum.toDouble else 0

    var totalStdLength: Double = 0
    var totalStdCount: Double = 0

    // println("keyCountLengthList:" + keyCountLengthList)

    keyCountLengthList.foreach { case (_, (count, length)) =>
      totalStdLength += math.pow(length - meanLength, 2)
      totalStdCount += math.pow(count - meanCount, 2)
    }

    val res = Array(totalNum, keyNum, totalLength,
      if (keyNum > 0) math.sqrt(totalStdLength / keyNum) else 0,
      if (keyNum > 0) math.sqrt(totalStdCount / keyNum) else 0)

    res
  }

  def print(): Unit = {
    return
    val res = calc()

    println("-------------------Block Stats Start--------------------")
    println("name:" + name)
    println("total:" + totalNum)
    println("keyNum:" + res(1))
    println("totalLength:" + totalLength)
    println("meanLength:" + res(3))
    println("meanCount:" + res(4))
    println("stdLength:" + res(5))
    println("stdCount:" + res(6))

    println("-------------------Block Stats End----------------------")

  }
}
