package com.libertexgroup.clickstream.models.messages

import com.libertexgroup.clickstream.models.parseQueryParams
import org.json4s.NoTypeHints
import org.json4s.native.Serialization
import org.json4s.native.Serialization.write

case class AdjustClickstream (
                clientId: Option[String],
                activityKind: String, // -> click,
                country: Option[String], // -> sg,
                gpsAdid: Option[String], // -> 0fec60dd-13ec-4813-b963-6c44f06b1620,
                idfa: Option[String], // -> 0FEC60DD-13EC-4813-B963-6C44F06B1620,
                trackerType: String, // -> idfa, gps_adid
                createdAt: String, // -> 1652011880,
                uriParamsJson: String
)

object AdjustClickstream {
  implicit val formats = Serialization.formats(NoTypeHints)
  def processClickstream(record: String): Option[AdjustClickstream] = {
    val parsed = parseQueryParams(record)
    if (parsed.nonEmpty) {
      Some(
        AdjustClickstream(
          clientId = parsed.get("client_id"),
          activityKind = parsed.getOrElse("activity_kind", ""),
          country = parsed.get("country"),
          createdAt = parsed.getOrElse("created_at", ""),
          gpsAdid = parsed.get("gps_adid"),
          idfa = parsed.get("idfa"),
          trackerType = if(parsed.contains("idfa") || parsed.getOrElse("idfa", "").isEmpty)
            TrackerType.Idfa.toString else TrackerType.GpsAdid.toString,
          //      uriParamsCompressed = Array.emptyByteArray
          uriParamsJson = write(parsed)
        ))
    } else {
      None
    }
  }
}
