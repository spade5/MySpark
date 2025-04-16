package org.apache.spark.util

import org.apache.spark.executor.InputMetrics
import org.apache.spark.storage.BlockId

sealed trait ReceiverTrackerMessage


case class TaskEnd(blockId: BlockId, host: String, executionTime: Long)
  extends ReceiverTrackerMessage


case class ReduceTaskEnd(rddId: Int, part: Int, host: String,
                         executionTime: Long, inputMetrics: InputMetrics)
  extends ReceiverTrackerMessage

case class AskBlockExtraInfo(blockId: BlockId)
  extends ReceiverTrackerMessage

case class AskBlockLocation(blockId: BlockId)
  extends ReceiverTrackerMessage

case class AskOtherBlockLocation(part: Int, rddID: Int)
  extends ReceiverTrackerMessage

case class AskExecutors()
  extends ReceiverTrackerMessage

case class AskStartTime()
  extends ReceiverTrackerMessage

case class UpdatePartKeyMap(partKeyMap: Map[String, Int])
  extends ReceiverTrackerMessage

case class GetPartKeyMap()
  extends ReceiverTrackerMessage

case class StartTime(startTime: Long, delay: Long)
  extends ReceiverTrackerMessage
