package org.hkust.BasedProcessFunctions

import org.apache.flink.annotation.Public
import org.apache.flink.api.common.state.ValueState
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector
import org.hkust.RelationType.Payload

import scala.collection.mutable.ArrayBuffer

/**
  * The based class for aggregation function in Cquirrel.  The process function will accept a join result and it calculate
  * the delta represents by the join result.  After applying the delta to the aggregate value on previous join result,
  * the process function will output the new aggregate value with all output attributes (if delta output option is on).
  *
  * @param name          the relation name that the CoProcessFunction will deal with.
  * @param thisKey       the current partition key of the process function.  If the aggregate function is connected with a
  *                      group by operation, then it will be set to the group by attributes, otherwise it will be set to empty.
  * @param outputKey     the output attributes that the query requires.
  * @param nextKey       Used to support more complex queries that requires to use aggregate result for more computation, in
  *                      that case, the aggregate function need to be distributed based on the key.
  * @param aggregateName a string used to indicate the aggregate column name, default value is "aggregate", a special
  *                      value "_multiple_" will be set if multiple columns are exists in the aggregate value (typically
  *                      happens when using user-defined aggregate type)
  * @param deltaOutput   a boolean value indicates whether to output each delta.  Default is set as ``false``
  * @param testMemory    @suspend whether to perform GC before output the memory data.  Default is set as ``false``
  * @tparam K   type of the key.
  * @tparam AGG type of the aggregate value.
  */
@Public
abstract class AggregateProcessFunction[K, AGG](name: String,
                                                thisKey: Array[String],
                                                outputKey: Array[String],
                                                nextKey: Array[String] = Array(),
                                                aggregateName: String = "aggregate",
                                                deltaOutput: Boolean = true,
                                                testMemory: Boolean = false)
  extends BasedProcessFunction[K, Payload, Payload](0, 0, name, testMemory) {

  /**
    * Used for more informative output.
    */
  override val prefix = "Aggregation Process Function"
  /**
    * The initial value of the aggregate value.  Usually set to be 0, might need to adjust based on the aggregate type.
    */
  val init_value: AGG
  private val tempArray: ArrayBuffer[Any] = new ArrayBuffer[Any]()
  /**
    * The state used to store the aggregate value.
    */
  var alive: ValueState[AGG] = _

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
  def addition(value1: AGG, value2: AGG): AGG

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
  def subtraction(value1: AGG, value2: AGG): AGG

  /**
    * Given a join tuple, the function will calculate an aggregate value.  Need to be defined basic on the aggregate
    * function of the given query.
    *
    * @param value the new join tuple.
    * @return the aggregate value.
    */
  def aggregate(value: Payload): AGG

  override def open(parameters: Configuration): Unit = {
    initstate()
    if (testMemory) {
      Runtime.getRuntime.runFinalization()
      Runtime.getRuntime.gc()
      Thread.sleep(30000)
      System.out.println(
        s"Aggregate Process Function Memory usage ${(Runtime.getRuntime.totalMemory() - Runtime.getRuntime.freeMemory()) / (1024 * 1024)}, EnumerationTime $OutputAccur, StorageTime $StoreAccur")
    }
  }

  override def process(value_raw: Payload, ctx: KeyedProcessFunction[K, Payload, Payload]#Context, out: Collector[Payload]): Unit = {
    // initial the state value with the ``init_value``
    if (alive.value() == null) alive.update(init_value)

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
            if (deltaOutput) {

              // Construct the output.
              if (aggregateName != "_multiple_") {
                val outputValue = setValue(value_raw, newValue.toString)
                value_raw._4 = outputValue
                value_raw._5 = outputKey :+ aggregateName
              } else {
                // TODO: Unfinished program, also getAggregate is unfinished
                val tempValue = getAggregate(newValue)
                value_raw._4 = setMultipleValue(value_raw, tempValue._1)
                value_raw._5 = outputKey ++ tempValue._2
              }
              value_raw._1 = "Output"
              value_raw._2 = "Output"
              value_raw.setKey(nextKey)

              if (isOutputValid(value_raw)) {
                out.collect(value_raw)
              }
            }
          }
        }
      case _ =>
    }
  }

  /**
    * Given a new join tuple, the function will decide whether the value is legal for aggregate.  In another word, the
    * function will be used for those selective conditions that involves two or more relations.  The default is set as
    * ``true``, which means that there is no such selective condition.  If so, the function need to override.
    *
    * <optimize>
    * some conditions might be able to push down, which does not require to construct the full join result.
    * For ease of implementation, we require those condition to be decided before doing the final aggregation.
    * </optimize>
    *
    * @param value the new join tuple.
    * @return whether the tuple can pass those selective conditions.
    */
  def isValid(value: Payload): Boolean = {
    true
  }

  /**
    * Given an aggregate value, the function will decide whether the value is legal for output.  In another word, the
    * function will be used for those selective conditions that involves the final aggregate values.  The default is set
    * as ``true``, which means that there is no such selective condition.  If so, the function need to override.
    *
    * @param value the new aggregate output.
    * @return whether the aggregation can pass those selective conditions.
    */
  def isOutputValid(value: Payload): Boolean = {
    true
  }

  def getAggregate(value: AGG): (Array[Any], Array[String]) = {
    (Array(), Array())
  }

  /**
    * A private function in [[AggregateProcessFunction]] to help construct the output tuple.  The function first copy
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

  override def expire(ctx: KeyedProcessFunction[K, Payload, Payload]#Context): Unit = {}

  override def storeStream(value: Payload, ctx: KeyedProcessFunction[K, Payload, Payload]#Context): Unit = {}

  override def testExists(value: Payload, ctx: KeyedProcessFunction[K, Payload, Payload]#Context): Boolean = {
    false
  }

  override def enumeration(out: Collector[Payload]): Unit = {}

  /**
    * A private function in [[AggregateProcessFunction]] to help construct the output tuple.  The function first copy
    * all output attributes into the output array, then it appends the aggregate at the end of the array.
    *
    * @param value_raw      the payload that contain all output attributes
    * @param aggregateValue the aggregation values, in an Any Array
    * @return an array that contains all output attributes and the aggregate value
    */
  private def setMultipleValue(value_raw: Payload, aggregateValue: Array[Any]): Array[Any] = {
    while (tempArray.nonEmpty) {

    }
    for (i <- outputKey) {
      tempArray.append(value_raw(i))
    }
    tempArray.appendAll(aggregateValue)
    val returnvalue = tempArray.toArray
    tempArray.clear()
    returnvalue
  }

}
