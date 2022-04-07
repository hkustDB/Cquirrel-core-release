package org.hkust.BasedProcessFunctions

import org.apache.flink.annotation.Public
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.common.typeinfo.{TypeHint, TypeInformation}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector
import org.hkust.RelationType.Payload
import org.hkust.Util

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
  * @author Qichen Wang
  * @since 2020/03/15
  *
  *        The based class for maintaining index structures in Cquirrel,  special designed for foreign-key primary-key joins.
  *        This class is analogous to [[RelationFKCoProcessFunction]], the main different is the
  *        [[RelationFKProcessFunction]] is designed to handle only one input stream, which means it is
  *        special for leaf nodes in Cquirrel.  Thus, there will be no input that contains state update operations.
  * @param name       the relation name that the CoProcessFunction will deal with.
  * @param thisKey    the current partition key of the process function. For a leaf node, it should be the primary key of
  *                   the relation.
  * @param nextKey    the partition key of the next process function.  For a leaf node, it should be the primary key of the
  *                   relation.
  * @param isRoot     used to indicate whether the relation is the root relation.
  * @param primaryKey The primary key of the relation.  The default setting is empty.
  * @param testMemory @suspend whether to perform GC before output the memory data.  Default is set as ``false``
  * @tparam K type of the key, usually it is Int, but it might be different if it is a composite key.
  */
@Public
abstract class RelationFKProcessFunction[K](name: String,
                                            thisKey: Array[String],
                                            nextKey: Array[String],
                                            isRoot: Boolean,
                                            primaryKey: Array[String] = Array(),
                                            testMemory: Boolean = false)
  extends BasedProcessFunction[K, Payload, Payload](0, 0, name, testMemory) {

  /**
    * Used for more informative output.
    */
  override val prefix = "Relational Process Function"
  /**
    * Used for format date type.
    */
  val format = Util.date_format

  var alive: ValueState[mutable.HashSet[Payload]] = _
  /**
    * @suspend A temp variable for redistributed.  Not valid now.
    */
  private var cnt = 0

  /**
    * For any incoming updates, determine whether the tuple can pass the select condition.
    *
    * @param value The incoming updates.
    * @return
    */
  def isValid(value: Payload): Boolean

  override def open(parameters: Configuration): Unit = {
    initstate()
    if (testMemory) {
      Runtime.getRuntime.runFinalization()
      Runtime.getRuntime.gc()
      Thread.sleep(30000)
      System.out.println(s"Process Function $name Parallelism ${getRuntimeContext.getIndexOfThisSubtask} Memory usage ${(Runtime.getRuntime.totalMemory() - Runtime.getRuntime.freeMemory()) / (1024 * 1024)}, EnumerationTime $OutputAccur, StorageTime $StoreAccur")
    }
  }

  override def initstate(): Unit = {
    val setDescriptor = TypeInformation.of(new TypeHint[mutable.HashSet[Payload]]() {})
    val aliveDescriptor: ValueStateDescriptor[mutable.HashSet[Payload]] = new ValueStateDescriptor[mutable.HashSet[Payload]](
      name + "Alive", setDescriptor
    )

    alive = getRuntimeContext.getState(aliveDescriptor)
  }

  override def process(value_raw: Payload, ctx: KeyedProcessFunction[K, Payload, Payload]#Context, out: Collector[Payload]): Unit = {
    if (alive.value() == null) {
      alive.update(new mutable.HashSet[Payload]())
    }

    def testOutput = {
      if (isOutputValid(value_raw)) out.collect(value_raw)
    }

    // @suspend Should not be used in next version as one input will not includes these updates.
    if (value_raw._2 == "SetAlive" || value_raw._2 == "SetOnHold") {
      throw new Exception("Receive Wrong Signals!")
    }

    if (value_raw._2 == "Insert" || value_raw._2 == "Delete") {

      if (value_raw._1 == name) {
        val value = Payload("Tuple", "Tuple", value_raw._3, value_raw._4, value_raw._5, value_raw._6)
        if (isValid(value)) {
          val temp = alive.value()
          if (value_raw._2 == "Insert") {
            /*
              Adding a new tuple into current set, and perform bottom-up update.
           */
            if (temp.add(value)) {
              if (isRoot) {
                value_raw.setKey(nextKey)
                value_raw._1 = "Aggregate"
                value_raw._2 = "Addition"
                testOutput
              } else {
                value_raw._1 = "SetKeyStatus"
                value_raw._2 = "SetAlive"
                value_raw._3 = setKey(value_raw._5, value_raw._4)
                testOutput
              }
            }
          } else {
            /*
              Removing a new tuple from current set, and perform bottom-up update.
           */
            if (temp.contains(value)) {
              if (isRoot) {
                value_raw.setKey(nextKey)
                value_raw._1 = "Aggregate"
                value_raw._2 = "Remove"
                testOutput
              } else {
                value_raw._1 = "SetKeyStatus"
                value_raw._2 = "SetOnHold"
                value_raw._3 = setKey(value_raw._5, value_raw._4)
                testOutput
              }
              temp.remove(value)
            }
          }

        }
      }
    }

  }

  private def setKey(attribute: Array[String], value: Array[Any]): Any = {
    val tempArray: ArrayBuffer[Any] = ArrayBuffer[Any]()
    if (nextKey.length != 0) {
      for (i <- nextKey.indices) {
        for (j <- attribute.indices)
          if (attribute(j) == nextKey(i)) tempArray += value(j)
        if (i == tempArray.length) {
          cnt = cnt + 1
          tempArray += cnt
        }
      }
    }
    tempArray.length match {
      case 0 => 1
      case 1 => tempArray(0)
      case 2 => Tuple2(tempArray(0), tempArray(1))
      case 3 => Tuple3(tempArray(0), tempArray(1), tempArray(2))
      case _ => throw new Exception("Not implemented yet!")
    }
  }

  /**
    *
    * @param value
    * @return
    */
  def isOutputValid(value: Payload): Boolean = {
    true
  }

  override def expire(ctx: KeyedProcessFunction[K, Payload, Payload]#Context): Unit = {}

  override def storeStream(value: Payload, ctx: KeyedProcessFunction[K, Payload, Payload]#Context): Unit = {}

  override def testExists(value: Payload, ctx: KeyedProcessFunction[K, Payload, Payload]#Context): Boolean = {
    false
  }

  override def enumeration(out: Collector[Payload]): Unit = {}
}
