package org.hkust.BasedProcessFunctions

import org.apache.flink.api.common.state.ValueState

import scala.collection.mutable

/**
  * The based class for Min/Max aggregation function in Cquirrel.  It overrides the addition function and subtraction
  * function.  For each insertion update, the aggregate function will record the update in state for further updates.
  * In addition, we use the algorithm that described in Yi et. al. ICDE 2003.
  *
  * @param name        the relation name that the CoProcessFunction will deal with.
  * @param thisKey     the current partition key of the process function.  If the aggregate function is connected with a
  *                    group by operation, then it will be set to the group by attributes, otherwise it will be set to empty.
  * @param outputKey   the output attributes that the query requires.
  * @param nextKey     Used to support more complex queries that requires to use aggregate result for more computation, in
  *                    that case, the aggregate function need to be distributed based on the key.
  * @param deltaOutput a boolean value indicates whether to output each delta.  Default is set as ``false``
  * @param testMemory  @suspend whether to perform GC before output the memory data.  Default is set as ``false``
  * @tparam K   type of the key.
  * @tparam AGG type of the aggregate value.
  */
abstract class MinMaxAggregateProcessFunction[K, AGG](name: String,
                                                      thisKey: Array[String],
                                                      outputKey: Array[String],
                                                      nextKey: Array[String] = Array(),
                                                      deltaOutput: Boolean = true,
                                                      testMemory: Boolean = false
                                                     )
  extends AggregateProcessFunction[K, AGG](name, thisKey, outputKey, nextKey = nextKey,
    deltaOutput = deltaOutput, testMemory = testMemory) {

  var buffer: ValueState[mutable.ArrayBuffer[AGG]] = _
  var second: ValueState[AGG] = _
  var third: ValueState[AGG] = _

  /**
    * Given an aggregate type, this function will returns the order of two aggregate values.  For example, if the
    * aggregate function is Max(), then the function should return true if ``value1`` > ``value2`` and false otherwise.
    *
    * IMPORTANT: The function need to check whether the value is null, in case the value is not initialized.
    *
    * @param value1
    * @param value2
    * @return if [[value1]] is prior than [[value2]] in the given order
    */
  def comparison(value1: AGG, value2: AGG): Boolean

  /**
    * Given an aggregate type, this function will determine whether two values are equal.  This is special for dealing
    * with float type, as two values might have slightly difference.
    *
    * @param value1
    * @param value2
    * @return if [[value1]] is equal to [[value2]]
    */
  def isEqual(value1: AGG, value2: AGG): Boolean

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
  override def addition(value1: AGG, value2: AGG): AGG = {
    if (buffer.value() == null) buffer.update(mutable.ArrayBuffer())
    // If new value is not prior than the original value
    if (comparison(value1, value2)) {
      // check if the new value is prior than the second value
      if (!comparison(second.value(), value2)) {
        // If new value is prior than the second value
        third.update(second.value())
        second.update(value2)
      } else {
        // check if the new value is prior than the third value
        if (!comparison(third.value(), value2)) {
          third.update(value2)
        }
      }
      buffer.value().append(value2)
      value1
    } else {
      // If the new value is prior than the original value.
      third.update(second.value())
      second.update(value1)
      buffer.value().append(value2)
      value2
    }
  }

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
  override def subtraction(value1: AGG, value2: AGG): AGG = {
    val i = buffer.value().indexWhere(x => isEqual(x, value2))
    buffer.value()(i) = buffer.value().last
    buffer.value().remove(buffer.value().length)
    var returnvalue: AGG = init_value
    // if the first value is removed
    if (isEqual(value1, value2)) {
      if (second.value() != null && second.value() != init_value) {
        returnvalue = second.value()
        second.update(third.value())
        third.update(init_value)
      }
    } else {
      returnvalue = value1
      // The second value is removed
      if (isEqual(second.value(), value2)) {
        if (third.value() != null && third.value != init_value) {
          second.update(third.value())
          third.update(init_value)
        }
      } else {
        // The third value is removed
        if (isEqual(third.value(), value2)) {
          third.update(init_value)
        }
      }
    }
    // All values are removed, reconstruct the index.
    if (isEqual(returnvalue, init_value)) {
      for (i <- buffer.value()) {
        if (comparison(i, alive.value())) {
          third.update(second.value())
          second.update(alive.value())
          alive.update(i)
        } else {
          if (comparison(i, second.value())) {
            third.update(second.value())
            second.update(i)
          } else {
            if (comparison(i, third.value())) {
              third.update(i)
            }
          }
        }
      }
      returnvalue = alive.value()
    }
    returnvalue
  }

}
