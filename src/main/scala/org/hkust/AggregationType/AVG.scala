package org.hkust.AggregationType

/**
  * Created by tom on 14/12/2020.
  * Copyright (c) 2020 tom
  */
class AVG(var sum: Double = 0.0, var count: Double = 0) extends Serializable {

  def this(init_value: Double) = {
    this(init_value, 1)
  }

  def +(value: AVG): AVG = {
    new AVG(sum + value.sum, count + value.count)
  }

  def -(value: AVG): AVG = {
    new AVG(sum - value.sum, count - value.count)
  }

  override def toString: String = {
    s"$sum, $count, ${this.apply()}"
  }

  def apply(): Double = {
    if ((sum - 0) * (sum - 0) > 1e-5 && (count - 0) * (count - 0) > 1e-5) sum / count
    else 0
  }
}
