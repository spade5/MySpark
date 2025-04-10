package org.apache.spark.streaming

import scala.collection.mutable.ArrayBuffer
// scalastyle:off println
class PerformSuite extends TestSuiteBase {
  val size = 100000
  val iterations = 10
  test("performance") {

    // 测试ArrayBuffer
    val arrayBufferTimes = (1 to iterations).map { _ =>
      val startTime = System.nanoTime()
      val buffer = new ArrayBuffer[Int]()

      // 测试追加操作
      for (i <- 0 until size) {
        buffer += i
      }

      // 测试随机访问
      var sum = 0
      for (i <- 0 until size) {
        sum += buffer(i)
      }

      // 测试中间插入
      for (i <- 0 until 1000) {
        buffer.insert(size/2, i)
      }

      val endTime = System.nanoTime()
      (endTime - startTime) / 1000000.0  // 转换为毫秒
    }

    // 测试List
    val listTimes = (1 to iterations).map { _ =>
      val startTime = System.nanoTime()
      var list = List[Int]()

      // 测试追加操作
      for (i <- 0 until size) {
        list = list :+ i
      }

      // 测试随机访问
      var sum = 0
      for (i <- 0 until size) {
        sum += list(i)
      }

      // 测试中间插入
      for (i <- 0 until 1000) {
        val (front, back) = list.splitAt(size/2)
        list = front ++ (i :: back)
      }

      val endTime = System.nanoTime()
      (endTime - startTime) / 1000000.0  // 转换为毫秒
    }

    // 计算平均时间
    val avgArrayBufferTime = arrayBufferTimes.sum / iterations
    val avgListTime = listTimes.sum / iterations

    println(s"测试结果 (${iterations}次迭代, 数据量${size}):")
    println(s"ArrayBuffer平均时间: ${avgArrayBufferTime}ms")
    println(s"List平均时间: ${avgListTime}ms")
    println(s"性能差异: ${avgListTime/avgArrayBufferTime}倍")

    // 验证结果
    assert(avgArrayBufferTime < avgListTime, "ArrayBuffer应该比List快")
  }

  test("memory") {

    // 测试ArrayBuffer内存使用
    val arrayBufferStartMemory = Runtime.getRuntime.totalMemory() - Runtime.getRuntime.freeMemory()
    val buffer = new ArrayBuffer[Int]()
    for (i <- 0 until size) {
      buffer += i
    }
    val arrayBufferEndMemory = Runtime.getRuntime.totalMemory() - Runtime.getRuntime.freeMemory()
    val arrayBufferMemory = arrayBufferEndMemory - arrayBufferStartMemory

    // 测试List内存使用
    val listStartMemory = Runtime.getRuntime.totalMemory() - Runtime.getRuntime.freeMemory()
    var list = List[Int]()
    for (i <- 0 until size) {
      list = list :+ i
    }
    val listEndMemory = Runtime.getRuntime.totalMemory() - Runtime.getRuntime.freeMemory()
    val listMemory = listEndMemory - listStartMemory

    println(s"内存使用对比 (数据量${size}):")
    println(s"ArrayBuffer内存使用: ${arrayBufferMemory/1024/1024}MB")
    println(s"List内存使用: ${listMemory/1024/1024}MB")
    println(s"内存使用差异: ${listMemory.toDouble/arrayBufferMemory}倍")

    // 验证结果
    assert(arrayBufferMemory < listMemory, "ArrayBuffer应该比List使用更少的内存")
  }

  test("GC") {
    // 测试ArrayBuffer的GC压力
    val arrayBufferGCTimes = (1 to iterations).map { _ =>
      val startTime = System.nanoTime()
      val buffer = new ArrayBuffer[Int]()

      // 频繁创建和销毁对象
      for (i <- 0 until size) {
        buffer += i
        if (i % 100000 == 0) {
          buffer.clear()
        }
      }

      val endTime = System.nanoTime()
      (endTime - startTime) / 1000000.0
    }

    // 测试List的GC压力
    val listGCTimes = (1 to iterations).map { _ =>
      val startTime = System.nanoTime()
      var list = List[Int]()

      // 频繁创建和销毁对象
      for (i <- 0 until size) {
        list = list :+ i
        if (i % 100000 == 0) {
          list = List.empty
        }
      }

      val endTime = System.nanoTime()
      (endTime - startTime) / 1000000.0
    }

    val avgArrayBufferGCTime = arrayBufferGCTimes.sum / iterations
    val avgListGCTime = listGCTimes.sum / iterations

    println(s"GC压力对比 (${iterations}次迭代, 数据量${size}):")
    println(s"ArrayBuffer平均GC时间: ${avgArrayBufferGCTime}ms")
    println(s"List平均GC时间: ${avgListGCTime}ms")
    println(s"GC压力差异: ${avgListGCTime/avgArrayBufferGCTime}倍")

    // 验证结果
    assert(avgArrayBufferGCTime < avgListGCTime, "ArrayBuffer应该比List产生更少的GC压力")
  }
}
