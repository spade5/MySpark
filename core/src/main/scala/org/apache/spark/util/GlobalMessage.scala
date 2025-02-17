package org.apache.spark.util

import org.apache.spark.storage.{BlockId}

sealed trait ReceiverTrackerMessage


case class TaskEnd(blockId: BlockId, executorId: String, executionTime: Long)
  extends ReceiverTrackerMessage
