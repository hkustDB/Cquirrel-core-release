package org.hkust.BasedProcessFunctions

import org.apache.flink.annotation.Public
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.common.typeinfo.{BasicTypeInfo, TypeHint, TypeInformation}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction
import org.apache.flink.util.Collector
import org.hkust.RelationType.Payload
import org.hkust.Util

import scala.collection.mutable

/**
  * The based class for maintaining index structures in Cquirrel, special designed for foreign-key primary-key joins.
  * This class is analogous to [[RelationFKProcessFunction]], the main different is the [[RelationFKCoProcessFunction]]
  * is designed to handle two input streams, one carries the insertion/deletion events, the other carries the state
  * update events from children nodes.  In addition, the function will append attributes from child node, thus it does
  * not require connect process function to enumerate output.
  *
  * @param name            the relation name that the CoProcessFunction will deal with.
  * @param maxC            a pre-set constant, indicates how many children need to be alive to make one key alive in current
  *                        relation.
  * @param thisKey         the current partition key of the process function.  For a non-leaf node, the value should be the join
  *                        keys between this node and its child node.
  * @param nextKey         the partition key of the next process function.
  * @param isRoot          used to indicate whether the relation is the root relation.
  * @param isLast          used to indicate whether the relation is the last process function of the relation.
  * @param primaryKey      The primary key of the relation.  The default setting is empty.
  * @param renameAttribute a String -> String map to store all attributes that need to be renamed, used to support AS clause.
  * @param id              a indicator to distinct different process functions that represents the same relation.  Used to avoid
  *                        conflict of state descriptor
  * @param testMemory      @suspend whether to perform GC before output the memory data.  Default is set as ``false``
  * @tparam K type of the key.
  */
@Public
abstract class RelationFKCoProcessFunction[K](name: String,
                                              maxC: Int,
                                              thisKey: Array[String],
                                              nextKey: Array[String],
                                              isRoot: Boolean,
                                              isLast: Boolean = false,
                                              primaryKey: Array[String] = Array(),
                                              renameAttribute: Map[String, String] = Map(),
                                              id: String = "0",
                                              testMemory: Boolean = false)
  extends BasedCoProcessFunction[K, Payload, Payload](0, 0, name, testMemory) {

  type Tattribute = (Array[String], Array[Any])
  /**
    * Used for more informative output.
    */
  override val prefix = "Relational Co Process Function"
  /**
    * Used for format date type.
    */
  val format = Util.date_format

  var alive: ValueState[mutable.HashSet[Payload]] = _

  var onholdCount: ValueState[Int] = _

  var statusTimestamp: ValueState[Long] = _
  /**
    * @suspend A temp variable for redistributed.  Not valid now.
    */
  private var cnt = 0
  /**
    * A special boolean state for handling the case for leaf node, in that case, multiple tuples can have same
    * primary (join) key, for each key, it should only send ''SetAlive''/''SetOnhold'' signal once.
    */
  private var isAlive: ValueState[Boolean] = _
  /**
    * A listState that stores all attributes that need to append to the current key, i.e.
    * $\pi_{att(D(R))} \sigma_{key = k} R \Join D(R) $.  As all tuples in the current partition will have the same key,
    * each appended attributes will have only one value.
    */
  private var additionAttributes: ValueState[Tattribute] = _

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
      System.out.println(s"Memory usage ${(Runtime.getRuntime.totalMemory() - Runtime.getRuntime.freeMemory()) / (1024 * 1024)}, EnumerationTime $OutputAccur, StorageTime $StoreAccur")
    }
  }

  override def initstate(): Unit = {
    val setDescriptor = TypeInformation.of(new TypeHint[mutable.HashSet[Payload]]() {})
    val aliveDescriptor: ValueStateDescriptor[mutable.HashSet[Payload]] = new ValueStateDescriptor[mutable.HashSet[Payload]](
      name + id + "Alive", setDescriptor
    )

    val countDescriptor: ValueStateDescriptor[Int] = new ValueStateDescriptor[Int](
      name + id + "Count", BasicTypeInfo.INT_TYPE_INFO.asInstanceOf[TypeInformation[Int]]
    )

    val boolDescriptor: ValueStateDescriptor[Boolean] = new ValueStateDescriptor[Boolean](
      name + id + "Boolean", BasicTypeInfo.BOOLEAN_TYPE_INFO.asInstanceOf[TypeInformation[Boolean]]
    )

    val timestampDescriptor: ValueStateDescriptor[Long] = new ValueStateDescriptor[Long](
      name + id + "TimeStamp", BasicTypeInfo.LONG_TYPE_INFO.asInstanceOf[TypeInformation[Long]]
    )

    val additionAttributesDescriptor: ValueStateDescriptor[Tattribute] = new ValueStateDescriptor[Tattribute](
      name + id + "additionAttributes", TypeInformation.of(new TypeHint[Tattribute]() {})
    )


    alive = getRuntimeContext.getState(aliveDescriptor)
    onholdCount = getRuntimeContext.getState(countDescriptor)
    isAlive = getRuntimeContext.getState(boolDescriptor)
    statusTimestamp = getRuntimeContext.getState(timestampDescriptor)
    additionAttributes = getRuntimeContext.getState(additionAttributesDescriptor)
  }

  override def process(value_raw: Payload, ctx: KeyedCoProcessFunction[K, Payload, Payload, Payload]#Context, out: Collector[Payload]): Unit = {
    if (alive.value() == null) {
      alive.update(new mutable.HashSet[Payload]())
      onholdCount.update(0)
      isAlive.update(false)
      statusTimestamp.update(0)
      additionAttributes.clear()
    }

    if (ctx.getCurrentKey != value_raw._3) {
      System.out.println("KeyValueNotMatching!")
      return
    }

    def SetOutput(i: Payload, timestamp: Long): Unit = {
      val tempAttribute = connectAttributes(i._5, i._4)
      val attr = additionAttributes.value()
      if (tempAttribute == null) {
        throw new Exception(s"ERROR! ${i._1}, ${i._2}, ${i._3}, ${i._4.mkString(" , ")}, ${i._5.mkString(" , ")} \n" +
          s"${value_raw._1}, ${value_raw._2}, ${value_raw._3}, ${value_raw._4.mkString(" , ")}, ${value_raw._5.mkString(" , ")}\n" +
          s"${attr._1.mkString(" , ")}, ${attr._2.mkString(" , ")}\n" +
          s"${ctx.getCurrentKey}")
      }
      value_raw._5 = tempAttribute._1
      value_raw._4 = tempAttribute._2
      value_raw._6 = Math.max(timestamp, i._6)
      value_raw.setKey(nextKey)
      if (isOutputValid(value_raw)) out.collect(value_raw)
    }

    if (value_raw._2 == "SetAlive" || value_raw._2 == "SetOnHold") {

      // Set correct timestamp.
      if (statusTimestamp.value() < value_raw._6)
        statusTimestamp.update(value_raw._6)

      if (value_raw._2 == "SetAlive") {
        val count = onholdCount.value() + 1
        onholdCount.update(count)

        // Append the new attributes into additionAttribute list
        additionAttributes.update((value_raw._5, value_raw._4))

        if (count == maxC) { // The current status change from onhold to alive
          /**
            * special handling if multiple elements has same primary key.  Such situation should not happen in PK-FK
            * joins, as the only use-case should be leaf nodes.
            */

          if (isEqual(thisKey, primaryKey)) {
            throw new UnknownError("The DAG contains some error, please check")
          } else {
            val tmp = alive.value()
            for (i <- tmp) {
              if (isRoot) {
                value_raw._1 = "Aggregate"
                value_raw._2 = "Addition"
                SetOutput(i, statusTimestamp.value())
              } else {
                /**
                  * Special handling for multiple foreign keys in one relation.  In that case, the process function
                  * should send an insert operation.
                  */
                if (!isLast) {
                  value_raw._1 = name
                  value_raw._2 = "Insert"
                }
                SetOutput(i, statusTimestamp.value())
              }
            }
          }
        }

      } else {
        val count = onholdCount.value() - 1
        onholdCount.update(count)
        if (count == maxC - 1) { // The current status change from alive to onhold
          statusTimestamp.update(0)
          //TODO this code might need to review
          /**
            * special handling if multiple elements has same primary key.  Such situation should not happen in PK-FK
            * joins, as the only use-case should be leaf nodes.
            */
          if (isEqual(thisKey, primaryKey)) { // special handling if multiple elements has same primary key.
            throw new UnknownError("The DAG contains some error, please check")
          } else {
            val tmp = alive.value()
            for (i <- tmp) {
              if (isRoot) {
                value_raw._1 = "Aggregate"
                value_raw._2 = "Remove"
                SetOutput(i, value_raw._6)
              } else {
                /**
                  * Special handling for multiple foreign keys in one relation.
                  */
                if (!isLast) {
                  value_raw._1 = name
                  value_raw._2 = "Delete"
                }
                SetOutput(i, value_raw._6)
              }
            }
          }
        }

        // Remove the attribute from the lists
        /*if (!removeAttributes(removedAttributes)) {
          throw new Exception("Removed Attributes are not in the list!")
        }*/
        additionAttributes.update(null)
      }

    }

    if (value_raw._2 == "Insert" || value_raw._2 == "Delete") {

      if (value_raw._1 == name) { // Test whether the relation is match with payload, should be redundant
        val value = new Payload(value_raw._6, value_raw._5, value_raw._4)
        if (isValid(value)) {
          val temp = alive.value()
          if (value_raw._2 == "Insert") {
            if (onholdCount.value() == maxC) { // The current status is alive
              /*
                Adding a new tuple into current set, and perform bottom-up update.
             */
              if (temp.add(value)) {
                if (isEqual(thisKey, primaryKey)) { // special handling if multiple elements has same primary key.
                  throw new UnknownError("The DAG contains some error, please check")
                } else {
                  if (isRoot) {
                    value_raw._1 = "Aggregate"
                    value_raw._2 = "Addition"
                    SetOutput(value_raw, statusTimestamp.value())
                  } else {
                    /**
                      * Special handling for multiple foreign keys in one relation.  If it is the last one, then the
                      * signal should change from "Insert" to "SetAlive"
                      */
                    if (isLast) {
                      value_raw._1 = "SetKeyStatus"
                      value_raw._2 = "SetAlive"
                    }
                    SetOutput(value_raw, statusTimestamp.value())
                  }
                }
              }
            } else {
              temp.add(value)
            }
          } else {
            if (onholdCount.value() == maxC) { // The current status is alive
              /*
                Removing a new tuple from current set, and perform bottom-up update.
             */
              if (temp.contains(value)) {
                if (isEqual(thisKey, primaryKey)) { // special handling if multiple elements has same primary key.
                  throw new UnknownError("The DAG contains some error, please check")

                } else {
                  if (isRoot) {
                    value_raw._1 = "Aggregate"
                    value_raw._2 = "Remove"
                    SetOutput(value_raw, statusTimestamp.value())
                  } else {
                    /**
                      * Special handling for multiple foreign keys in one relation.
                      */
                    if (isLast) {
                      value_raw._1 = "SetKeyStatus"
                      value_raw._2 = "SetOnHold"
                    }
                    SetOutput(value_raw, statusTimestamp.value())
                  }
                }
                temp.remove(value)
              }
            } else {
              temp.remove(value)
            }
          }

        }
      }
    }

  }

  private def isEqual(s1: Array[String], s2: Array[String]): Boolean = {
    if (s1.length != s2.length)
      return false
    for (i <- s1) {
      if (!s2.contains(i)) return false
    }
    true
  }

  /**
    *
    * @param value
    * @return
    */
  def isOutputValid(value: Payload): Boolean = {
    true
  }

  private def connectAttributes(attribute: Array[String], value: Array[Any]): Tattribute = {
    val newAttribute: mutable.ArrayBuffer[String] = new mutable.ArrayBuffer[String]() ++= attribute
    val newValue: mutable.ArrayBuffer[Any] = new mutable.ArrayBuffer[Any]() ++= value

    val attr = additionAttributes.value()

    for (i <- attr._1.indices) {
      val j = newAttribute.indexOf(attr._1(i))
      // If the new attribute is not exists in the attribute list, then add it into the buffer
      if (j == -1) {
        newAttribute.append(attr._1(i))
        newValue.append(attr._2(i))
      } else {
        // If the attribute already exists in the buffer and the value not match -> auxiliary attribute not match,
        // return null as indicator
        if (!newValue(j).equals(attr._2(i))) {
          return null
        }
      }
    }


    (newAttribute.toArray, newValue.toArray)

  }

  override def expire(ctx: KeyedCoProcessFunction[K, Payload, Payload, Payload]#Context): Unit = {}

  override def storeStream(value: Payload, ctx: KeyedCoProcessFunction[K, Payload, Payload, Payload]#Context): Unit = {}

  override def testExists(value: Payload, ctx: KeyedCoProcessFunction[K, Payload, Payload, Payload]#Context): Boolean = {
    false
  }

  override def enumeration(out: Collector[Payload]): Unit = {}

}
