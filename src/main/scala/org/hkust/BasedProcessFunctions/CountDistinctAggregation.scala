package org.hkust.BasedProcessFunctions
import org.apache.flink.api.common.state.{MapState, MapStateDescriptor, ValueStateDescriptor}
import org.apache.flink.api.common.typeinfo.{TypeHint, TypeInformation}
import org.hkust.RelationType.Payload

import scala.collection.mutable

/**
  * Created by Qichen Wang
  * Copyright (c) 2020 HKUST Theory Group
  */
class CountDistinctAggregation[K](distinctAttr: Array[String],
                               name: String,
                               thisKey: Array[String],
                               outputKey: Array[String],
                               nextKey: Array[String] = Array(),
                               aggregateName: String = "aggregate",
                               deltaOutput: Boolean = true,
                               testMemory: Boolean = false)
  extends AggregateProcessFunction[K, Int](name, thisKey, outputKey, nextKey, aggregateName, deltaOutput, testMemory)  {
  /**
    * The initial value of the aggregate value.  Usually set to be 0, might need to adjust based on the aggregate type.
    */
  override val init_value: Int = 0

  override val prefix = "CountDistinct Process Function"

  var distinctState : MapState[Any, Int] = _

  /**
    * Given an aggregate type, this function performs addition.  The addition operation is not just the numerical
    * addition.  One simple example is for MIN/MAX aggregation, the addition operation should be set to min() or max().
    * It is defined by the (semi-)ring of the given aggregate function.  In another word, the function will return the
    * result after applying the update (insertion) to the old aggregate result.
    *
    * @param value1 the first value in [[AGG]] type, which contains the old aggregate result.
    * @param value2 the second value in [[AGG]] type, which contains the delta result brought by the update.
    * @return output the value of value1 $oplus$ value2.
    */
  override def addition(value1: Int, value2: Int): Int = value1+value2

  /**
    * Given an aggregate type, this function performs subtraction.  Same as the addition function, the subtraction is
    * not just the numerical subtraction.  The subtraction can be difficult than addition, as the addition operation
    * is defined on both semi-ring and ring, but subtraction only defines on semi-ring, if the aggregate function is a
    * ring (like MIN/MAX), the subtraction need to do extra work (like scan the entire alive join results).
    * The function will return the result after applying the update (deletion) to the old aggregate result.
    *
    * @param value1 the first value in [[AGG]] type, which contains the old aggregate result.
    * @param value2 the second value in [[AGG]] type, which contains the delta result brought by the update.
    * @return output the value of value1 $ominus$ value2.
    */
  override def subtraction(value1: Int, value2: Int): Int = value1-value2

  /**
    * Given a join tuple, the function will calculate an aggregate value.  Need to be defined basic on the aggregate
    * function of the given query.
    *
    * @param value the new join tuple.
    * @return the aggregate value.
    */
  override def aggregate(value: Payload): Int = {
    if (value._2 == "Addition") {
      val key = getAttributeValue(value)
      if (distinctState.contains(key)) {
        val temp : Int = distinctState.get(key)
        distinctState.put(key, temp+1)
        0
      } else {
        distinctState.put(key, 1)
        1
      }
    } else {
      if (value._2 == "Remove") {
        val key = getAttributeValue(value)
        if (distinctState.contains(key)) {
          val temp : Int = distinctState.get(key)
          if (temp == 1) {
            distinctState.remove(key)
            1
          } else {
            distinctState.put(key, temp-1)
            0
          }
        } else {
          throw new Exception("Element not exists! " + value._4.mkString(" , "))
        }
      } else {
        throw new Exception("Error! the update type is either Addition nor Deletion!")
      }
    }
  }

  private def getAttributeValue(value : Payload) : Any = {
    val newAttribute: mutable.ArrayBuffer[Any] = mutable.ArrayBuffer()
    for (i<- distinctAttr) {
      val j = value._5.indexOf(i)
      if (j == -1) throw new Exception("ERROR! Index not found! " + distinctAttr.mkString(" , ") + value._5.mkString(" , "))
      newAttribute.append(value._4(j))
    }
    if (newAttribute.size == 1) return newAttribute(0).asInstanceOf[Any]
    if (newAttribute.size == 2) return (newAttribute(0), newAttribute(1))
    if (newAttribute.size == 3) return (newAttribute(0), newAttribute(1), newAttribute(2))
    if (newAttribute.size > 3) throw new Exception("Distinct count with more than 4 attributes are not supported.")
  }

  /**
    * A function to initial the state as Flink required.
    */
  override def initstate(): Unit = {
    val valueDescriptor = TypeInformation.of(new TypeHint[Int](){})
    val aliveDescriptor : ValueStateDescriptor[Int] = new ValueStateDescriptor[Int](
      name+"Alive", valueDescriptor
    )
    val arrayDescriptor = TypeInformation.of(new TypeHint[Any](){})
    val mapDescriptor : MapStateDescriptor[Any, Int] = new MapStateDescriptor[Any, Int](
      name+"Map", arrayDescriptor, valueDescriptor
    )
    alive = getRuntimeContext.getState(aliveDescriptor)
    distinctState = getRuntimeContext.getMapState[Any, Int](mapDescriptor)
  }
}
