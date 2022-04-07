package org.hkust.RelationType

import org.hkust.Util

import scala.collection.mutable.ArrayBuffer

/**
  * This is the base class for a tuple or an operation.  It will be transfer through the Flink DAG and store into the
  * states.
  *
  * @param _1 the relation name or the operation name.  Currently there are five types of support operation:
  *           ``Aggregate`` a partial/full join result that needs to construct the full join result via
  *           connect process function / calculate the aggregate result via aggregate process function.
  *           ``Output``   a final output tuple, only use for the final output.
  *           ``SetKeyStatus`` an operation that set the status of corresponding key that the payload, transferred
  *           through 'Index Update' paths.
  *           ``Tuple``    a tuple, only use in internal state.
  *           $RELATION_NAME$  an update to the corresponding relation.
  * @param _2 the operation type.  Currently there are six types.  ``SetAlive`` / ``SetOnHold`` means the operation want
  *           to set the corresponding key to alive or non-alive.  ``Insert`` / ``Delete`` means the corresponding tuple
  *           need to be inserted or deleted from the relation.  ``Addition`` / ``Remove`` means the new aggregate value
  *           should be added / subtracted from the current aggregate value.
  * @param _3 the partition key used in the distributed environment, need to be set before the payload being sent to
  *           next operation.
  * @param _4 an array for storing attribute value, using [[Any]] as the type.
  * @param _5 an array for storing attribute name.  For each attribute, it should have the same index in _4 and _5.
  * @param _6 a timestamp for handling late arrive elements. The initial value of this field should be the number
  *           the input tuple.
  */
case class Payload(var _1: String,
                   var _2: String,
                   var _3: Any,
                   var _4: Array[Any],
                   var _5: Array[String],
                   var _6: Long) extends Serializable {

  /**
    * A private variable for store the value of primary key to reduce reconstruction.
    */
  private var primaryKey: Array[Any] = null

  /**
    * An alternative construct function, specially used when we need to store a tuple in internal state.
    *
    * @param timeStamp  same as [[_6]]
    * @param keyArray   same as [[_5]]
    * @param valueArray saame as [[_4]]
    */
  def this(timeStamp: Long, keyArray: Array[String], valueArray: Array[Any]) = {
    this("Tuple", "Tuple", 0, valueArray, keyArray, timeStamp)
  }

  /**
    * Compare two payloads to decide if they represents the same tuple / key.
    * It will test all attributes in current payload to see whether the other payload has the same attribute, and it
    * will compare their value of those attributes to see if they are equal.  If so, then it will output true, otherwise
    * false.
    * TODO  decide whether the comparison need to be strictly equal, e.g. [[obj]] is exactly the same as [[this]].
    * current version only requires the [[obj]] be a super set of [[this]]
    *
    * @param obj the object needs to compare.
    * @return whether the [[obj]] and [[this]] has same attributes and same values.
    */
  override def equals(obj: Any): Boolean = {
    if (obj.getClass == this.getClass) {
      for (i <- _5.indices) {
        val j = obj.asInstanceOf[this.type]._5.indexOf(_5(i))
        if (j == -1) return false
        if (obj.asInstanceOf[this.type]._4(j) != _4(i)) return false
      }
      true
    } else {
      false
    }
  }

  /**
    * A hash code function.  Since the current comparison only on the attribute values, thus it returns the hashCode of
    * [[_5]]
    *
    * @return a hash code.
    */
  override def hashCode(): Int = {
    _5.toSeq.hashCode()
  }

  /**
    * Setting the partition key [[_3]] with the given key array.
    *
    * @param nextKey an array represents the attributes in the next partition key.
    */

  def setKey(nextKey: Array[String]): Unit = {
    _3 = findKey(nextKey)
  }

  /**
    * Given a partition key, return the corresponding value in the payload of those attributes.  The return value is
    * in Tuple type.  If one or more attributes in the partition key is not exists in the payload, then it will return
    * random partition key.
    *
    * @param keyArray an array represents the attributes in the next partition key.
    * @return a tuple (or an Int) of the partition key
    */
  def findKey(keyArray: Array[String]): Any = {
    var isContain: Boolean = true
    if (keyArray.length != 0) {
      for (i <- keyArray) {
        if (!this.contains(i)) isContain = false
      }
    }
    if (isContain) {
      keyArray.length match {
        case 0 => 1
        case 1 => this.apply(keyArray(0))
        case 2 => Tuple2(this.apply(keyArray(0)), this.apply(keyArray(1)))
        case 3 => Tuple3(this.apply(keyArray(0)), this.apply(keyArray(1)), this.apply(keyArray(2)))
        case 4 => Tuple4(this.apply(keyArray(0)), this.apply(keyArray(1)), this.apply(keyArray(2)), this.apply(keyArray(3)))
        case _ => throw new UnsupportedOperationException("Current not support!")
      }
    } else {
      /**
        * @deprecated Currently it should not happen.
        */
      throw new NoSuchElementException(s"Key not found, ${_5.mkString(" , ")}, ${_4.mkString(" , ")}, ${keyArray.mkString(" , ")}")
    }
  }

  /**
    * Given an attribute name, decide whether the tuple / key / partial (full) join result contain the attribute.
    *
    * @param attr a String for the attribute name
    * @return whether the attribute is existed.
    */
  def contains(attr: String): Boolean = {
    if (_5.contains(attr)) true
    else false
  }

  /**
    * An easy used api for retrieving a value of given attribute in current payload.
    *
    * @param attr a string of required attribute name
    * @return the value of that attribute
    * @throws NullPointerException if the attribute does not exists in the payload.
    *
    */
  def apply(attr: String): Any = {
    val i = _5.indexOf(attr)
    if (i != -1)
      _4(i)
    else
      throw new NullPointerException(
        "Null Point error! Try to Get " + attr + " from " + _1)
  }

  /**
    * Return the value of primary key.
    *
    * @param _primarykey The array that store the primary key names.
    * @return An array that store the primary key value.
    */
  def getPrimaryKey(_primarykey: Array[String]): Array[Any] = {
    if (primaryKey == null) primaryKey = getAttributes(_primarykey)
    primaryKey
  }

  /**
    * Return the value of a set of attributes.
    *
    * @param attr_name The array that store the attribute names.
    * @return An array that store all attribute values.
    */
  def getAttributes(attr_name: Array[String]): Array[Any] = {
    val tempArray: ArrayBuffer[Any] = new ArrayBuffer[Any]()
    for (i <- attr_name) {
      tempArray.append(this (i))
    }
    tempArray.toArray
  }

  /**
    * Modify the attribute names to support AS clause.
    *
    * @param _orig the original name, in String.
    * @param _new  the new name, in String.
    */
  def rename(_orig: String, _new: String): Unit = {
    val index = _5.indexOf(_orig)
    if (index == -1)
      throw new NoSuchElementException(s"The Database Scheme does not contain attribute ${_orig}")
    _5(index) = _new
  }

}
