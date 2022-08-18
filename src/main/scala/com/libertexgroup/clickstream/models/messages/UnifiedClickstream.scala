package com.libertexgroup.clickstream.models.messages

import com.libertexgroup.clickstream.models.tryParseDate

case class UnifiedClickstream(
                               clientId: Option[String],
                               eventType: Option[String],
                               country: Option[String],
                               broker: Option[String],
                               sessionId: Option[String],
                               visitorId: Option[String],
                               gpsAdid: Option[String], // -> 0fec60dd-13ec-4813-b963-6c44f06b1620,
                               idfa: Option[String], // -> 0FEC60DD-13EC-4813-B963-6C44F06B1620,
                               trackerType: String, // -> idfa, gps_adid
                               createdAt: java.sql.Timestamp, // -> 1652011880,
                               origin: String,
                               uriParamsJson: String
                             )

object UnifiedClickstream {
  def fromAudienceClickstream(record: AudienceClickstream): UnifiedClickstream =
    UnifiedClickstream(
      clientId = record.clientId,
      eventType = record.eventType.map(_.toLowerCase),
      country = record.country.map(_.toLowerCase),
      broker = record.broker,
      sessionId = record.sessionId,
      visitorId = record.visitorId,
      gpsAdid = None,
      idfa = None,
      trackerType = TrackerType.Session_Visitor.toString,
      createdAt = record.eventDate,
      origin = "topicaudience",
      uriParamsJson = record.uriParamsJson
    )

  def fromAdjustClickstream(record: AdjustClickstream): UnifiedClickstream =
    UnifiedClickstream(
      clientId = record.clientId,
      eventType = Some(record.activityKind.toLowerCase),
      country = record.country.map(_.toLowerCase),
      broker = None,
      sessionId = None,
      visitorId = None,
      gpsAdid = record.gpsAdid,
      idfa = record.idfa,
      trackerType = record.trackerType,
      createdAt = tryParseDate(Some(record.createdAt)),
      origin = "adjust",
      uriParamsJson = record.uriParamsJson
    )
}