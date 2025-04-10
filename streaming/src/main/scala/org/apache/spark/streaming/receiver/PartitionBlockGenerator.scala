/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.streaming.receiver

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

import org.apache.spark.{SparkConf, SparkEnv, SparkException}
import org.apache.spark.rdd.BlockStats
import org.apache.spark.storage.{BlockId, StreamBlockId}
import org.apache.spark.streaming.scheduler.{AddBlockExtraInfo, ExecutorCacheTaskLocationWithMetrics}
import org.apache.spark.streaming.util.{FreqAVLTree, RecurringTimer, Regression}
import org.apache.spark.util.{AskExecutors, AskStartTime, Clock, RpcUtils, StartTime, SystemClock}


/** Listener object for BlockGenerator events */

/**
 * Generates batches of objects received by a
 * [[org.apache.spark.streaming.receiver.Receiver]] and puts them into appropriately
 * named blocks at regular intervals. This class starts two threads,
 * one to periodically start a new batch and prepare the previous batch of as a block,
 * the other to push the blocks into the block manager.
 *
 * Note: Do not create BlockGenerator instances directly inside receivers. Use
 * `ReceiverSupervisor.createBlockGenerator` to create a BlockGenerator and use it.
 */

private[streaming] case class BlockExtraInfo(blockId: BlockId,
                                             blockStats: BlockStats[Any],
                                             nextPartitionId: Option[Int],
                                             hostInfo: Option[Seq[Float]],
                                             host: Option[String],
                                             executorId: Option[String],
                                             blockTime: Float = 0)

private[streaming] class PartitionBlockGenerator (
     listener: BlockGeneratorListener,
     receiverId: Int,
     conf: SparkConf,
     env: SparkEnv,
     clock: Clock = new SystemClock()
   ) extends BlockGenerator(listener, receiverId, conf, clock) {
  import GeneratorState._


  // scalastyle:off println
  println("PartitionBlockGenerator")

  /* private val blockIntervalMs = conf.get(BLOCK_INTERVAL)
  require(blockIntervalMs > 0, s"'${BLOCK_INTERVAL.key}' should be a positive value")

  private val blockIntervalTimer =
    new RecurringTimer(clock, blockIntervalMs, updateCurrentBuffer, "BlockGenerator") */

  // private val maxRecordsPerBatch = conf.get("MAX_RECORDS_PER_BATCH", "1000").toInt
  // private var endpoint: RpcEndpointRef = null
  private lazy val trackerEndpoint = RpcUtils.makeDriverRef("ReceiverTracker", env.conf, env.rpcEnv)
  private var blockIntervalTimer: RecurringTimer = null
  private val queue = new mutable.Queue[FreqAVLTree[Any]]
  private var count = 0
  private val numMappers = conf.get("spark.default.parallelism", "3").toInt - 1
  private val regression = new Regression(conf)

  def curAvlTree: FreqAVLTree[Any] = {
    if (queue.isEmpty) {
      queue += new FreqAVLTree[Any](true)
    }
    queue.last
  }

  def dequeueAvlTree(): FreqAVLTree[Any] = {
    if (queue.isEmpty) {
      queue += new FreqAVLTree[Any](true)
    }
    queue.dequeue()
  }

  private val repeat = conf.get("spark.streaming.repeat", "10").toInt
  override def addData(data: Any): Unit = {
    if (state == Active) {
      waitToPush()
      synchronized {
        if (state == Active) {
          // currentBuffer += data
          data match {
            case str: String =>
              val splits = str.split(Array(' ', '\t', '\n', '\r'))
              var i = 0
              while (i < splits.length) {
                val split = splits(i)
                if (split.nonEmpty) {
                  curAvlTree.insert(split, repeat)
                  count += repeat
                }
                i += 1
              }
            case _ =>
              curAvlTree.insert(data)
              count += 1
          }

          /* if (count >= maxRecordsPerBatch) {
            count = 0
            queue += new FreqAVLTree[Any](true)
          } */
        } else {
          throw new SparkException(
            "Cannot add data as BlockGenerator has not been started or has been stopped")
        }
      }
    } else {
      throw new SparkException(
        "Cannot add data as BlockGenerator has not been started or has been stopped")
    }
  }

  override def start(): Unit = synchronized {
    if (state == Initialized) {
      state = Active
      blockPushingThread.start()
      logInfo("Started PartitionBlockGenerator")
    } else {
      throw new SparkException(
        s"Cannot start BlockGenerator as its not in the Initialized state [state = $state]")
    }

    logInfo("Starting BlockGenerator, tracker:" + trackerEndpoint.address)

    trackerEndpoint.askSync[StartTime](AskStartTime) match {
      case StartTime(startTime, delay) =>
        blockIntervalTimer = new RecurringTimer(clock, delay, updateCurrentBuffer,
          "PartitionBlockGenerator")
        // blockIntervalTimer.start(startTime + (delay * 0.95).toLong)
        blockIntervalTimer.start()
      case _ =>
    }


    /* endpoint = env.rpcEnv.setupEndpoint(
      "PartitionBlockGenerator-endpoint", new ThreadSafeRpcEndpoint {
        override val rpcEnv: RpcEnv = env.rpcEnv

        override def receive: PartialFunction[Any, Unit] = {
          case SubmitJobs(delay) =>
            if (blockIntervalTimer == null) {
              println("SubmitJobs:" + delay)
              blockIntervalTimer = new RecurringTimer(clock, delay, updateCurrentBuffer,
                "PartitionBlockGenerator")
              blockIntervalTimer.start(clock.getTimeMillis() + (delay * 0.95).toLong)
            }
        }
      })
    println("endpoint blockGenerator:" + endpoint.address) */
  }

  /**
   * Stop everything in the right order such that all the data added is pushed out correctly.
   *
   *  - First, stop adding data to the current buffer.
   *  - Second, stop generating blocks.
   *  - Finally, wait for queue of to-be-pushed blocks to be drained.
   */
  override def stop(): Unit = {
    // Set the state to stop adding data
    synchronized {
      if (state == Active) {
        state = StoppedAddingData
      } else {
        logWarning(s"Cannot stop BlockGenerator as its not in the Active state [state = $state]")
        return
      }
    }

    // Stop generating blocks and set the state for block pushing thread to start draining the queue
    logInfo("Stopping BlockGenerator")
    synchronized { state = StoppedGeneratingBlocks }

    // Wait for the queue to drain and mark state as StoppedAll
    logInfo("Waiting for block pushing thread to terminate")
    blockPushingThread.join()
    synchronized { state = StoppedAll }
    logInfo("Stopped BlockGenerator")

    /* env.rpcEnv.stop(
       endpoint
    ) */
  }

  /** Change the buffer to which single records are added to. */
  override def updateCurrentBuffer(time: Long): Unit = {
    println("updateCurrentBuffer:" + time)
    logInfo(s"Updating current buffer for time $time, count=$count ")
    println(s"Updating current buffer for time $time, count=$count ")
    try {
      var newBlocks: List[Block] = null
      synchronized {
        /* if (currentBuffer.nonEmpty) {
          val newBlockBuffer = currentBuffer
          currentBuffer = new ArrayBuffer[Any]
          val blockId = StreamBlockId(receiverId, time)
          listener.onGenerateBlock(blockId)
          newBlock = new Block(blockId, newBlockBuffer)
        } */
        val freqAVLTree = dequeueAvlTree()
        if (!freqAVLTree.empty) {
          val bufferList = freqAVLTree.getAllData
          println(s"total=${freqAVLTree.getTotal} bufferList.size=${bufferList.size}")
          freqAVLTree.clear()
          count = 0
          newBlocks = generateBlocks(time, bufferList)
        }
      }

      if (newBlocks != null) {
        newBlocks.foreach(newBlock => {
          blocksForPushing.put(newBlock)
        })
      }
    } catch {
      case ie: InterruptedException =>
        logInfo("Block updating timer thread was interrupted")
      case e: Exception =>
        reportError("Error in block updating thread", e)
    }
  }

  def generateBlocks(time: Long, bufferList: List[ArrayBuffer[Any]]): List[Block] = {
    conf.get("spark.streaming.blockGeneratorStyle", "prompt") match {
      case "prompt" =>
        generateBlockPromptStyle(time, bufferList)
      case "regression" =>
        generateBlockRegressionStyle(time, bufferList)
      case _ =>
        generateBlockPromptStyle(time, bufferList)
    }
  }

  private def generateBlockPromptStyle(time: Long,
                                       bufferList: List[ArrayBuffer[Any]]): List[Block] = {
    // 提前获取executor信息
    val splitExecutors = trackerEndpoint.
      askSync[Seq[Seq[ExecutorCacheTaskLocationWithMetrics]]](AskExecutors)

    val candidates = splitExecutors.head.slice(0, 0 + numMappers)
    
    val buffers = List.fill(numMappers)(new ArrayBuffer[Any]())
    val blockStats = List.fill(numMappers)(new BlockStats[Any]())

    var i = 0
    var step = 1
    // 将 bufferList 中的数据按照 Z 字形依次添加到 buffers 中
    // println("bufferList:" + bufferList)
     println(s"bufferList.size=${bufferList.size}, " +
      s"bufferList.length=${bufferList.length}, total=${bufferList.map(_.size).sum}")
    bufferList.foreach((buffer) => {
      buffers(i) ++= buffer
      blockStats(i).insert(buffer(0), buffer.size)
      if (i == 0) {
        step = 1
      } else if (i == numMappers - 1) {
        step = -1
      }
      i = (i + step)
    })
    /* println("buffers:" + buffers)
    blockStats.foreach{(blockStat: BlockStats[Any]) => {
      println("blockStat:" + blockStat.calc().mkString("Array(", ", ", ")"))
    }} */

    buffers.filter(_.nonEmpty).zipWithIndex.map{
      case (buffer, index) =>
        val blockId = StreamBlockId(receiverId, time + index)
        if (candidates.nonEmpty) {
          val executorInfo = candidates.lift(index % candidates.size)
          val extraInfo = BlockExtraInfo(
            blockId,
            blockStats(index),
            None,
            executorInfo.map(_.metrics),
            executorInfo.map(_.host),
            executorInfo.map(_.executorId),
          )
          trackerEndpoint.send(AddBlockExtraInfo(extraInfo))
        }
        listener.onGenerateBlock(blockId)
        Block(blockId, buffer, None, Some(index))
    }
  }

  private case class MergedStats[T](stats: BlockStats[T], buffer: ArrayBuffer[T],
                                    var count: Int = 0)

  private def mergeStats(bufferList: List[ArrayBuffer[Any]], nodeCount: Int):
  List[MergedStats[Any]] = {
    val startTime = clock.getTimeMillis()

    val gran_factor = conf.get("spark.streaming.granularityFactor", "50").toInt
    val mergedStats = new ArrayBuffer[MergedStats[Any]]()
    val totalSize = bufferList.map(_.size).sum
    val threshold = totalSize / Math.max(gran_factor * nodeCount, 10)
    
    bufferList.foreach { buffer =>
      if (mergedStats.isEmpty || mergedStats.last.count > threshold) {
        mergedStats += MergedStats(new BlockStats[Any](), new ArrayBuffer[Any]())
      }
      mergedStats.last.buffer ++= buffer
      mergedStats.last.stats.insert(buffer(0), buffer.size)
      mergedStats.last.count += buffer.size
    }

    logInfo(s"Merge stats time: ${clock.getTimeMillis() - startTime}")

    mergedStats.toList
  }

  private def generateBlockRegressionStyle(time: Long,
                                           bufferList: List[ArrayBuffer[Any]]): List[Block] = {
    if (!regression.ready) {
      logError("Regression model is not ready, use prompt style")
      return generateBlockPromptStyle(time, bufferList)
    }

    logInfo("Regression style")

    val startTime = clock.getTimeMillis()

    val splitExecutors = trackerEndpoint.
      askSync[Seq[Seq[ExecutorCacheTaskLocationWithMetrics]]](AskExecutors)

    val candidates = splitExecutors.head.slice(0, 0 + numMappers)

    if (candidates.isEmpty) {
      logError("No executors available, use prompt style")
      return generateBlockPromptStyle(time, bufferList)
    }

    logInfo("Candidates:" + candidates.map(_.host).mkString(","))

    val numBlock = math.min(numMappers, candidates.size)

    val buffers = List.fill(numBlock)(new ArrayBuffer[Any]())
    val blockStats = List.fill(numBlock)(new BlockStats[Any]())
    val mergedStatsList = mergeStats(bufferList, numBlock)

    logInfo("mergedStats:" + mergedStatsList.size)

    val predTimes = Array.fill(numBlock)(0.0f)

    mergedStatsList.foreach((mergedStats) => {
      val buffer = mergedStats.buffer
      val stats = mergedStats.stats
      var best: Option[Int] = None
      var minTime: Float = Long.MaxValue
      val featureList = ArrayBuffer[Array[Float]]()
      for (i <- 0 until numBlock) {
        val newBlockStats = blockStats(i).copy()
        // newBlockStats.insert(buffer(0), buffer.size)
        newBlockStats.merge(stats)
        val features: Array[Float] = newBlockStats.calc() ++ candidates(i).metrics
        featureList += features
      }

      // logInfo(s"Feature list size: ${featureList.length}")
      // logInfo("Feature list:" + featureList.map(_.mkString(",")).mkString("\n"))
      val times = regression.predict(featureList.toArray)
      // logInfo("times:" + times)

      times match {
        case Some(ts) =>
          // 找出最佳预测结果// 找出最佳预测结果
          for (i <- ts.indices) {
            if (ts(i) < minTime) {
              minTime = ts(i)
              best = Some(i)
            }
          }
        case _ =>
      }

      // logInfo("best:" + best)
      // logInfo("minTime:" + minTime)

      val target = best match {
        case Some(i) =>
          i
        case _ =>
          logWarning("Prediction failed, using round-robin assignment")
          buffer.size % numBlock
      }

      buffers(target) ++= buffer
      blockStats(target).merge(stats)
      predTimes(target) = minTime
    })

    // logInfo("buffers:" + buffers.map(_.size).mkString(","))

    val blocks = buffers.zipWithIndex.map{
      case (buffer, index) =>
        if (buffer.isEmpty) {
          null
        } else {
          val blockId = StreamBlockId(receiverId, time + index)
          val extraInfo = BlockExtraInfo(
            blockId,
            blockStats(index),
            None,
            Some(candidates(index).metrics),
            Some(candidates(index).host),
            Some(candidates(index).executorId),
            predTimes(index)
          )
          listener.onGenerateBlock(blockId)
          trackerEndpoint.send(AddBlockExtraInfo(extraInfo))
          Block(blockId, buffer, None, Some(index))
        }
    }.filter(_ != null)

    val timeTaken = clock.getTimeMillis() - startTime
    logInfo(s"[PartitionTime]$time:$timeTaken")
    println(s"Time taken to generate blocks $time:" + timeTaken + " ms")

    blocks
  }
}
