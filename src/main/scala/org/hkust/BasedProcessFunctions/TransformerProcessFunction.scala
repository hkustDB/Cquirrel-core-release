package org.hkust.BasedProcessFunctions

import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector
import org.hkust.RelationType.Payload

import scala.collection.mutable.ArrayBuffer

/**
  * Created by tom on 2021/7/20.
  * Copyright (c) 2021 tom 
  */
abstract class TransformerProcessFunction[K, AGG] (//name: String,
                                     //thisKey: Array[String],
                                     outputKey: Array[String],
                                     //nextKey: Array[String] = Array(),
                                     aggregateName: String = "aggregate",
                                     //deltaOutput: Boolean = true,
                                     testMemory: Boolean = false)
  extends BasedProcessFunction[K, Payload, Payload](0, 0, "transformer", testMemory) {

  private val tempArray: ArrayBuffer[Any] = new ArrayBuffer[Any]()

  /**
    * A function to initial the state as Flink required.
    */
  override def initstate(): Unit = {}

  /**
    * @deprecated
    * A function to enumerate the current join result.
    * @param out the output collector.
    */
  override def enumeration(out: Collector[Payload]): Unit = {}

  /**
    * A function to deal with expired elements.
    *
    * @param ctx the current keyed context
    */
  override def expire(ctx: KeyedProcessFunction[K, Payload, Payload]#Context): Unit = {}

  def expr(value : Payload) : AGG

  /**
    * A private function in [[TransformerProcessFunction]] to help construct the output tuple.  The function first copy
    * all output attributes into the output array, then it appends the aggregate at the end of the array.
    *
    * @param value_raw      the payload that contain all output attributes
    * @param aggregateValue the aggregation value
    * @return an array that contains all output attributes and the aggregate value
    */
  def setValue(value_raw: Payload, aggregateValue: Any): Array[Any] = {
    while (tempArray.nonEmpty) {

    }
    for (i <- outputKey) {
      tempArray.append(value_raw(i))
    }
    tempArray.append(aggregateValue)
    val returnvalue = tempArray.toArray
    tempArray.clear()
    returnvalue
  }

  /**
    * A function to process new input element.
    *
    * @param value_raw the raw value of current insert element
    * @param ctx       the current keyed context
    * @param out       the output collector, to collect the output stream.
    */
  override def process(value_raw: Payload, ctx: KeyedProcessFunction[K, Payload, Payload]#Context, out: Collector[Payload]): Unit = {
    var value : AGG = expr(value_raw)
    val outputValue = setValue(value_raw, value.toString)
    value_raw._4 = outputValue
    value_raw._5 = outputKey :+ aggregateName
    value_raw._1 = "Output"
    value_raw._2 = "Output"
    //value_raw.setKey(nextKey)
    out.collect(value_raw)
  }

  /**
    * @deprecated
    * A function to store the elements in current time window into the state, for expired.
    * @param value the raw value of current insert element
    * @param ctx   the current keyed context
    */
  override def storeStream(value: Payload, ctx: KeyedProcessFunction[K, Payload, Payload]#Context): Unit = {}

  /**
    * @deprecated
    * A function to test whether the new element is already processed or the new element is legal for process.
    * @param value the raw value of current insert element
    * @param ctx   the current keyed context
    * @return a boolean value whether the new element need to be processed.
    */
  override def testExists(value: Payload, ctx: KeyedProcessFunction[K, Payload, Payload]#Context): Boolean = {
    false
  }
}
