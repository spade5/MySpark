package org.apache.spark.util

import org.apache.spark.storage.BlockId

sealed trait ReceiverTrackerMessage


case class TaskEnd(blockId: BlockId, host: String, executionTime: Long)
  extends ReceiverTrackerMessage

case class AskBlockExtraInfo(blockId: BlockId)
  extends ReceiverTrackerMessage

case class AskBlockLocation(blockId: BlockId)
  extends ReceiverTrackerMessage

case class AskOtherBlockLocation()
  extends ReceiverTrackerMessage

case class AskExecutors()
  extends ReceiverTrackerMessage

case class AskStartTime()
  extends ReceiverTrackerMessage

case class StartTime(startTime: Long, delay: Long)
  extends ReceiverTrackerMessage
