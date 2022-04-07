package org.hkust.BasedProcessFunctions

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.common.typeinfo.{TypeHint, TypeInformation}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector
import org.hkust.RelationType.Payload

import scala.collection.mutable

/**
  * Created by Qichen Wang
  * Copyright (c) 2020 HKUST Theory Group
  */
abstract class StoredAggregateProcessFunction[K, AGG](name: String,
                                                      thisKey: Array[String],
                                                      outputKey: Array[String],
                                                      nextKey: Array[String] = Array(),
                                                      aggregateName: String = "aggregate",
                                                      deltaOutput: Boolean = true,
                                                      testMemory: Boolean = false)
  extends AggregateProcessFunction[K, AGG](name, thisKey, outputKey, nextKey, aggregateName, deltaOutput, testMemory) {

  override val prefix = "Stored Aggregate Process Function"
  var aliveTuples: ValueState[mutable.HashSet[Payload]] = _
  var nonAliveTuples: ValueState[mutable.HashSet[Payload]] = _

  override def open(parameters: Configuration): Unit = {
    initPayloadState()
    initstate()
    if (testMemory) {
      Runtime.getRuntime.runFinalization()
      Runtime.getRuntime.gc()
      Thread.sleep(30000)
      System.out.println(
        s"Stored Aggregate Process Function Memory usage ${(Runtime.getRuntime.totalMemory() - Runtime.getRuntime.freeMemory()) / (1024 * 1024)}, EnumerationTime $OutputAccur, StorageTime $StoreAccur")
    }
  }

  private def initPayloadState(): Unit = {
    val setDescriptor = TypeInformation.of(new TypeHint[mutable.HashSet[Payload]]() {})
    val aliveDescriptor: ValueStateDescriptor[mutable.HashSet[Payload]] =
      new ValueStateDescriptor[mutable.HashSet[Payload]](
        name + "StoreAggregateAlive", setDescriptor
      )
    val nonAliveDescriptor: ValueStateDescriptor[mutable.HashSet[Payload]] =
      new ValueStateDescriptor[mutable.HashSet[Payload]](
        name + "StoreAggregateNonAlive", setDescriptor
      )
    aliveTuples = getRuntimeContext.getState(aliveDescriptor)
    nonAliveTuples = getRuntimeContext.getState(nonAliveDescriptor)
  }

  def getAggregateValue: AGG = {
    alive.value()
  }

  override def process(value_raw: Payload, ctx: KeyedProcessFunction[K, Payload, Payload]#Context, out: Collector[Payload]): Unit = {
    // initial the state value with the ``init_value``
    if (alive.value() == null) {
      alive.update(init_value)
      aliveTuples.update(mutable.HashSet[Payload]())
      nonAliveTuples.update(mutable.HashSet[Payload]())
    }

    value_raw._1 match {
      case "Aggregate" =>

        // There will be two aggregation type, one is addition, the other is subtraction.
        if (value_raw._2 != "Addition" && value_raw._2 != "Remove") {

        } else {
          if (isValid(value_raw)) {

            // Calculate the delta and apply to old aggregate value.
            val delta: AGG = aggregate(value_raw)
            val newValue = if (value_raw._2 == "Addition") {
              addition(alive.value(), delta)
            } else {
              if (value_raw._2 == "Remove") subtraction(alive.value(), delta)
              else throw new Exception("Unknown Error")
            }

            alive.update(newValue)

            // Handling Second Step on the newly inserted tuple.

            if (value_raw._2 == "Addition") {
              if (aliveCondition(value_raw)) {
                aliveTuples.value().add(value_raw)
                value_raw.setKey(nextKey)
                out.collect(value_raw)
              } else {
                nonAliveTuples.value().add(value_raw)
              }
            } else {
              if (aliveTuples.value().contains(value_raw)) {
                aliveTuples.value().remove(value_raw)
                value_raw.setKey(nextKey)
                out.collect(value_raw)
              } else {
                if (!nonAliveTuples.value().remove(value_raw)) {
                  throw new NoSuchElementException("No Elements! " + value_raw)
                }
              }
            }

            // Handling Third Step

            testAliveAndNonAlive(out, value_raw._6)


          }
        }
      case _ =>
    }
  }

  private def testAliveAndNonAlive(out: Collector[Payload], currentTimeStamp: Long): Unit = {
    val tempAlive = aliveTuples.value()
    val tempNonAlive = nonAliveTuples.value()
    for (i <- tempAlive) {
      if (!aliveCondition(i)) {
        /**
          * To construct the output. WARNING: the timestamp is not checked.
          * For current version, we assume the timestamp is fully corrected.
          * If not, the update still need to be made as previous error is
          * already apply in the index.  Dropping the update cannot reconstruct
          * the correct output.
          * */
        tempAlive.remove(i)
        i._1 = "Aggregate"
        i._2 = "Remove"
        i.setKey(nextKey)
        if (i._6 > currentTimeStamp) {
          System.out.println("A timestamp conflict is detected.")
        }
        i._6 = currentTimeStamp
        out.collect(i)
        tempNonAlive.add(i)
      }
    }
    for (i <- tempNonAlive) {
      if (aliveCondition(i)) {
        /**
          * To construct the output. WARNING: the timestamp is not checked.
          * For current version, we assume the timestamp is fully corrected.
          * If not, the update still need to be made as previous error is
          * already apply in the index.  Dropping the update cannot reconstruct
          * the correct output.
          * */
        tempNonAlive.remove(i)
        i._1 = "Aggregate"
        i._2 = "Addition"
        i.setKey(nextKey)
        if (i._6 > currentTimeStamp) {
          System.out.println("A timestamp conflict is detected.")
        }
        i._6 = currentTimeStamp
        out.collect(i)
        tempAlive.add(i)
      }
    }
  }

  def aliveCondition(value: Payload): Boolean = {
    true
  }

}
