package org.hkust

/**
  * Created by tom on 11/3/2020.
  * Copyright (c) 2020 tom
  */
package object Util {
  val date_format = new java.text.SimpleDateFormat("yyyy-MM-dd")
  val t1 = date_format.parse("1994-12-31")
  val t2 = date_format.parse("1997-01-01")
  var keyCount = 0
}
