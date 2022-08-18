package com.libertexgroup.clickstream.models.messages

object TrackerType extends Enumeration {
  type Type = Value

  val Session_Visitor, GpsAdid, Idfa = Value
}
