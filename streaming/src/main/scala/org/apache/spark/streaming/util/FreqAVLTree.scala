
package org.apache.spark.streaming.util

import java.util.{Timer, TimerTask}

import scala.annotation.tailrec
import scala.collection.mutable

private[streaming] class FreqAVLTree[K](
    keepData: Boolean = false,
    updateIntervalMs: Long = 500,
    updateCount: Int = 500) extends Serializable {
  private var root: AVLTree[K] = Empty
  private val keyMap: mutable.HashMap[K, Node[K]] = mutable.HashMap()
  private val keyListMap: mutable.HashMap[K, mutable.ArrayBuffer[K]] = mutable.HashMap()
  private val keyMapToUpdate: mutable.HashMap[K, Int] = mutable.HashMap()
  private var currentCount: Int = 0
  var updateTimes: Int = 0

  val timer = new Timer()
  timer.schedule(new TimerTask {
    override def run(): Unit = {
      update()
    }
  }, updateIntervalMs, updateIntervalMs)

  def insert(key: K): Unit = {
    currentCount += 1
    keyMapToUpdate(key) = keyMapToUpdate.getOrElse(key, 0) + 1

    if (keepData) {
      keyListMap.getOrElseUpdate(key, mutable.ArrayBuffer()).append(key)
    }

    if (currentCount >= updateCount) {
      update()
    }
  }

  def empty(): Boolean = root == Empty

  def clear(): Unit = {
    root = Empty
    keyMap.clear()
    keyListMap.clear()
    keyMapToUpdate.clear()
    currentCount = 0
  }

  private def update(): Unit = {
    updateTimes += 1
    synchronized {
      if (keyMapToUpdate.nonEmpty) {
        keyMapToUpdate.keys.foreach(key => {
          if (keyMap.contains(key)) {
            val value = keyMap(key).value
            root = delete(root, value)
            keyMapToUpdate(key) += value.frequency
            keyMap -= key
          }
          root = insert(root, KeyFreqValue(key, keyMapToUpdate(key)))
        })

        keyMapToUpdate.clear()
      }
      currentCount = 0
    }
  }

  def getAllKeyFreqs: List[KeyFreqValue[K]] = {
    update()
    inOrder(root)
  }

  def getAllData: List[mutable.ArrayBuffer[K]] = {
    getAllKeyFreqs.map {
      case KeyFreqValue(key, _) => keyListMap.getOrElse(key, mutable.ArrayBuffer())
    }
  }

  private def height(tree: AVLTree[K]): Int = tree match {
    case Empty => 0
    case Node(_, _, _, h) => h
  }

  private def balanceFactor(tree: AVLTree[K]): Int = tree match {
    case Empty => 0
    case Node(_, left, right, _) => height(left) - height(right)
  }

  private def rotateLeft(node: Node[K]): AVLTree[K] = node match {
    case Node(value, left, Node(rightValue, rightLeft, rightRight, _), _) =>
      val newLeft = Node(value, left, rightLeft, Math.max(height(left), height(rightLeft)) + 1)
      Node(rightValue, newLeft, rightRight, Math.max(height(newLeft), height(rightRight)) + 1)
    case _ => node
  }

  private def rotateRight(node: Node[K]): AVLTree[K] = node match {
    case Node(value, Node(leftValue, leftLeft, leftRight, _), right, _) =>
      val newRight = Node(value, leftRight, right, Math.max(height(leftRight), height(right)) + 1)
      Node(leftValue, leftLeft, newRight, Math.max(height(leftLeft), height(newRight)) + 1)
    case _ => node
  }

  private def balance(tree: AVLTree[K]): AVLTree[K] = tree match {
    case Empty => Empty
    case node @ Node(_, left, right, _) =>
      val factor = balanceFactor(node)
      if (factor > 1) {
        // 左子树不平衡
        if (balanceFactor(left) < 0) {
          Node(node.value, rotateLeft(left.asInstanceOf[Node[K]]), right, node.height)
        }
        rotateRight(node)
      } else if (factor < -1) {
        // 右子树不平衡
        if (balanceFactor(right) > 0) {
          Node(node.value, left, rotateRight(right.asInstanceOf[Node[K]]), node.height)
        }
        rotateLeft(node)
      } else {
        // 树已平衡
        node
      }
  }

  private def insert(tree: AVLTree[K], value: KeyFreqValue[K]): AVLTree[K] = tree match {
    case Empty =>
      keyMap.getOrElseUpdate(value.key, Node(value, Empty, Empty, 1))
    case Node(v, left, right, _) if value.frequency <= v.frequency =>
      balance(Node(v, insert(left, value), right, Math.max(height(left), height(right)) + 1))
    case Node(v, left, right, _) if value.frequency > v.frequency =>
      balance(Node(v, left, insert(right, value), Math.max(height(left), height(right)) + 1))
  }

  private def delete(tree: AVLTree[K], value: KeyFreqValue[K]): AVLTree[K] = tree match {
    case Empty => Empty
    case Node(v, left, right, _) if (value.frequency < v.frequency ||
      value.frequency == v.frequency && value.key != v.key) =>
      balance(Node(v, delete(left, value), right, Math.max(height(left), height(right)) + 1))
    case Node(v, left, right, _) if value.frequency > v.frequency =>
      balance(Node(v, left, delete(right, value), Math.max(height(left) + 1, height(right))))
    case Node(v, left, right, _) => (left, right) match {
        case (Empty, Empty) => Empty
        case (Empty, _) => right
        case (_, Empty) => left
        case _ => // 如果有两个子树，找到右子树的最小值替代当前节点
          val minValue = findMin(right)
          balance(Node(minValue, left, delete(right, minValue),
            Math.max(height(left), height(right)) + 1))
      }
  }

  @tailrec
  private def findMin(tree: AVLTree[K]): KeyFreqValue[K] = tree match {
    case Node(value, Empty, _, _) => value
    case Node(_, left, _, _) => findMin(left)
    case Empty => throw new NoSuchElementException("Tree is empty")
  }

  private def inOrder(tree: AVLTree[K]): List[KeyFreqValue[K]] = tree match {
    case Empty => Nil
    case Node(value, left, right, _) => inOrder(right) ++ List(value) ++ inOrder(left)
  }

  def close(): Unit = {
    timer.cancel()
  }
}

private[streaming] case class KeyFreqValue[T](key: T, frequency: Int)

sealed trait AVLTree[+A]
case object Empty extends AVLTree[Nothing]
case class Node[A](
                    value: KeyFreqValue[A],
                    left: AVLTree[A],
                    right: AVLTree[A],
                    height: Int
                  ) extends AVLTree[A]

// scalastyle:off println
object FreqAVLTreeTest extends App {
  private val freqAVLTree: FreqAVLTree[String] = new FreqAVLTree[String](true, 1000, 10)

  val words = List("hello", "hello", "hello", "hello1", "hello",
    "hello", "hello", "hello", "hello", "hello", "world", "word", "hello", "word",
    "world", "world")

  words.foreach(key => freqAVLTree.insert(key))
  println(freqAVLTree.updateTimes)
  println(freqAVLTree.getAllKeyFreqs)
  println(freqAVLTree.updateTimes)
  Thread.sleep(2000)
  println(freqAVLTree.updateTimes)
  freqAVLTree.close()
  Thread.sleep(2000)
  println(freqAVLTree.updateTimes)

}
