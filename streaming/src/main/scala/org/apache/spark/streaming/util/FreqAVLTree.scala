
package org.apache.spark.streaming.util

import java.util.{Timer, TimerTask}
import scala.collection.mutable

private[streaming] class FreqAVLTree[K](
    keepData: Boolean = false,
    updateIntervalMs: Long = 500,
    updateCount: Int = 2000) extends Serializable {
  private var root: AVLTree[K] = Empty
  private val keyMap: mutable.HashMap[K, Node[K]] = mutable.HashMap()
  private val keyListMap: mutable.HashMap[K, mutable.ArrayBuffer[K]] = mutable.HashMap()
  private val keyMapToUpdate: mutable.HashMap[K, Int] = mutable.HashMap()
  private var currentCount: Int = 0
  private var total = 0
  var updateTimes: Int = 0

  val timer = new Timer()
  timer.schedule(new TimerTask {
    override def run(): Unit = {
      update()
    }
  }, updateIntervalMs, updateIntervalMs)

  def insert(key: K, count: Int = 1): Unit = {
    synchronized {
      currentCount += count
      total += count
      keyMapToUpdate(key) = keyMapToUpdate.getOrElse(key, 0) + count

      if (keepData) {
        keyListMap.getOrElseUpdate(key, mutable.ArrayBuffer()).appendAll(List.fill(count)(key))
        // println(s"total data length = ${keyListMap.toList.map(_._2.length).sum}")
      }

      if (currentCount >= updateCount) {
        update()
      }
    }
  }

  def empty(): Boolean = root == Empty

  def clear(): Unit = {
    root = Empty
    keyMap.clear()
    keyListMap.clear()
    keyMapToUpdate.clear()
    currentCount = 0
    total = 0
  }

  def getTotal: Int = total

  private def update(): Unit = {
    updateTimes += 1
    synchronized {
      // println(s"Update $updateTimes currentCount=$currentCount keylength=${keyMapToUpdate.size}")
      if (keyMapToUpdate.nonEmpty) {
        keyMapToUpdate.keys.foreach(key => {
          var value = keyMapToUpdate(key)
          if (keyMap.contains(key)) {
            val curNode = keyMap(key)
            value += curNode.value

            curNode.keys -= key
          }
          root = insert(root, KeyFreqValue(key, value))
          // println(root)

          // println(inOrder(root).map(item => s"(${item.key}, ${item.frequency})").mkString(", "))
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
    // println(getAllKeyFreqs.map(item => s"(${item.key}, ${item.frequency})").mkString(", "))
    getAllKeyFreqs.map {
      case KeyFreqValue(key, _) => keyListMap.getOrElse(key, mutable.ArrayBuffer())
    }
  }

  private def height(tree: AVLTree[K]): Int = tree match {
    case Empty => 0
    case Node(_, _, _, h, _) => h
  }

  private def balanceFactor(tree: AVLTree[K]): Int = tree match {
    case Empty => 0
    case Node(_, left, right, _, _) => height(left) - height(right)
  }

  private def rotateLeft(node: Node[K]): AVLTree[K] = node match {
    case Node(value, left, Node(rightValue, rightLeft, rightRight, _, rightKeys), _, keys) =>
      val newLeft = Node(value, left, rightLeft, Math.max(height(left),
        height(rightLeft)) + 1, keys)
      Node(rightValue, newLeft, rightRight, Math.max(height(newLeft),
        height(rightRight)) + 1, rightKeys)
    case _ => node
  }

  private def rotateRight(node: Node[K]): AVLTree[K] = node match {
    case Node(value, Node(leftValue, leftLeft, leftRight, _, leftKeys), right, _, keys) =>
      val newRight = Node(value, leftRight, right, Math.max(height(leftRight),
        height(right)) + 1, keys)
      Node(leftValue, leftLeft, newRight, Math.max(height(leftLeft),
        height(newRight)) + 1, leftKeys)
    case _ => node
  }

  private def balance(tree: AVLTree[K]): AVLTree[K] = tree match {
    case Empty => Empty
    case node @ Node(_, left, right, _, keys) =>
      val factor = balanceFactor(node)
      if (factor > 1) {
        // 左子树不平衡
        if (balanceFactor(left) < 0) {
          Node(node.value, rotateLeft(left.asInstanceOf[Node[K]]), right, node.height, keys)
        }
        rotateRight(node)
      } else if (factor < -1) {
        // 右子树不平衡
        if (balanceFactor(right) > 0) {
          Node(node.value, left, rotateRight(right.asInstanceOf[Node[K]]), node.height, keys)
        }
        rotateLeft(node)
      } else {
        // 树已平衡
        node
      }
  }

  private def insert(tree: AVLTree[K], value: KeyFreqValue[K]): AVLTree[K] = tree match {
    case Empty =>
      // println(s"Insert ${value.frequency} into empty tree")
      val hashMap = mutable.HashMap({value.key -> true})
      val node = Node(value.frequency, Empty, Empty, 1, hashMap)
      keyMap(value.key) = node
      node
    case Node(v, left, right, _, keys) if value.frequency < v =>
      // println(s"Insert ${value.frequency} into left of $v")
      balance(Node(v, insert(left, value), right, Math.max(height(left), height(right)) + 1, keys))
    case Node(v, left, right, _, keys) if value.frequency > v =>
      // println(s"Insert ${value.frequency} into right of $v")
      balance(Node(v, left, insert(right, value), Math.max(height(left), height(right)) + 1, keys))
    case Node(_, _, _, _, keys) =>
      keys(value.key) = true
      // println(s"Insert ${value.frequency} into current node")
      keyMap(value.key) = tree.asInstanceOf[Node[K]]
      tree
  }

  private def inOrder(tree: AVLTree[K]): List[KeyFreqValue[K]] = tree match {
    case Empty => Nil
    case Node(value, left, right, _, keys) => inOrder(right) ++
      keys.toList.map(item => KeyFreqValue(item._1, value)) ++ inOrder(left)
  }

  def close(): Unit = {
    timer.cancel()
  }
}

private[streaming] case class KeyFreqValue[T](key: T, frequency: Int)

sealed trait AVLTree[+A]
case object Empty extends AVLTree[Nothing]
case class Node[A](
                    value: Int,
                    left: AVLTree[A],
                    right: AVLTree[A],
                    height: Int,
                    keys: mutable.HashMap[A, Boolean] = mutable.HashMap()
                  ) extends AVLTree[A]

// scalastyle:off println
object FreqAVLTreeTest extends App {
  private val freqAVLTree: FreqAVLTree[String] = new FreqAVLTree[String](true)

  val words = List("nytpolitics", "Finally", "some", "great", "news", "hope", "they",
    "lose", "The", "latest", "The", "SPORTS", "Daily", "Thanks", "to", "anaesthete",
    "chachieseva", "sports", "news", "davidmweissman", "RonFilipkowski", "I", "just",
    "hope", "that", "whatever", "opened", "your", "eyes", "from")

  words.slice(0, 10).foreach(key => freqAVLTree.insert(key))
  println(freqAVLTree.updateTimes)
  println(freqAVLTree.getAllKeyFreqs)
  println(freqAVLTree.updateTimes)

  println(freqAVLTree.getAllData.map(arr => (arr(0), arr.length, arr.size)))

  Thread.sleep(1000)

  words.slice(10, 20).foreach(key => freqAVLTree.insert(key))
  println(freqAVLTree.updateTimes)
  println(freqAVLTree.getAllKeyFreqs)

  println(freqAVLTree.getAllData.map(arr => (arr(0), arr.length, arr.size)))

  freqAVLTree.close()
  Thread.sleep(1000)

  words.slice(20, 30).foreach(key => freqAVLTree.insert(key))
  println(freqAVLTree.updateTimes)
  println(freqAVLTree.getAllKeyFreqs)

  println(freqAVLTree.getAllData.map(arr => (arr(0), arr.length, arr.size)))

}
