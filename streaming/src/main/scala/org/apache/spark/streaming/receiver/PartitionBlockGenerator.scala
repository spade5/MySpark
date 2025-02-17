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

import org.apache.spark.rdd.BlockStats

import scala.collection.mutable.ArrayBuffer
import org.apache.spark.{SparkConf, SparkEnv, SparkException}
import org.apache.spark.rpc.{RpcEndpointRef, RpcEnv, ThreadSafeRpcEndpoint}
import org.apache.spark.storage.StreamBlockId
import org.apache.spark.streaming.scheduler.AddBlockExtraInfo
import org.apache.spark.streaming.util.{FreqAVLTree, RecurringTimer, Regression}
import org.apache.spark.util.{Clock, RpcUtils, SystemClock}


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

private[streaming] case class BlockExtraInfo(blockId: StreamBlockId,
                                             blockStats: BlockStats[Any],
                                             nextPartitionId: Option[Int],
                                             executorId: Option[String])

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

  private var endpoint: RpcEndpointRef = null
  private val trackerEndpoint = RpcUtils.makeDriverRef("ReceiverTracker", env.conf, env.rpcEnv)
  private var blockIntervalTimer: RecurringTimer = null
  private val freqAVLTree = new FreqAVLTree[Any](true)
  private val numMappers = 3
  private val regression = new Regression()

  override def addData(data: Any): Unit = {
    if (state == Active) {
      waitToPush()
      synchronized {
        if (state == Active) {
          // currentBuffer += data
          freqAVLTree.insert(data)
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

    endpoint = env.rpcEnv.setupEndpoint(
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
    println("endpoint blockGenerator:" + endpoint.address)
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

    env.rpcEnv.stop(
      endpoint
    )
  }

  /** Change the buffer to which single records are added to. */
  override def updateCurrentBuffer(time: Long): Unit = {
    println("updateCurrentBuffer:" + time)
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
        if (!freqAVLTree.empty) {
          val bufferList = freqAVLTree.getAllData
          freqAVLTree.clear()
          newBlocks = generateBlocks(time, bufferList)
        }
      }

      if (newBlocks != null) {
        newBlocks.foreach(newBlock => blocksForPushing.put(newBlock))
      }
    } catch {
      case ie: InterruptedException =>
        logInfo("Block updating timer thread was interrupted")
      case e: Exception =>
        reportError("Error in block updating thread", e)
    }
  }

  def generateBlocks(time: Long, bufferList: List[ArrayBuffer[Any]]): List[Block] = {
    generateBlockPromptStyle(time, bufferList)
  }

  private def generateBlockPromptStyle(time: Long,
                                       bufferList: List[ArrayBuffer[Any]]): List[Block] = {
    val buffers = List.fill(numMappers)(new ArrayBuffer[Any]())
    val blockStats = List.fill(numMappers)(new BlockStats[Any]())

    var i = 0
    var step = 1
    // 将 bufferList 中的数据按照 Z 字形依次添加到 buffers 中
    // println("bufferList:" + bufferList)
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
        listener.onGenerateBlock(blockId)
        trackerEndpoint.send(
          AddBlockExtraInfo(BlockExtraInfo(blockId, blockStats(index), None, None)))
        Block(blockId, buffer, None, Some(index))
    }
  }

  private def generateBlockRegressionStyle(time: Long,
                                           bufferList: List[ArrayBuffer[Any]]): List[Block] = {
    if (!regression.ready) {
      return generateBlockPromptStyle(time, bufferList)
    }

    val buffers = List.fill(numMappers)(new ArrayBuffer[Any]())

    buffers.filter(_.nonEmpty).zipWithIndex.map{
      case (buffer, index) =>
        val blockId = StreamBlockId(receiverId, time + index)
        listener.onGenerateBlock(blockId)
        Block(blockId, buffer, None, Some(index))
    }
  }
}
