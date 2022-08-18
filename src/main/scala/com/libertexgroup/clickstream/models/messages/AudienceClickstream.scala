package com.libertexgroup.clickstream.models.messages
import com.libertexgroup.clickstream.models.{parseQueryParams, tryParseDate}
import org.json4s.NoTypeHints
import org.json4s.native.Serialization
import org.json4s.native.Serialization.write

case class AudienceClickstream (
  clientId: Option[String],
  broker: Option[String],
  country: Option[String],
  eventType: Option[String],
  sessionId: Option[String],
  visitorId: Option[String],
  eventDate: java.sql.Timestamp,
  uriParamsJson: String
)

object AudienceClickstream {
  implicit val formats = Serialization.formats(NoTypeHints)
  def processClickstream(record: String): Option[AudienceClickstream] = {
    val parsed = parseQueryParams(record)
    if (parsed.nonEmpty) {
      Some(
        AudienceClickstream(
          clientId = parsed.get("customer_id"),
          broker = parsed.get("customer_profile_broker"),
          country = parsed.get("customer_profile_country"),
          eventType = parsed.get("event_type"),
          sessionId = parsed.get("session_id"),
          visitorId = parsed.get("visitor_id"),
          eventDate = tryParseDate(parsed.get("event_date_utc")),
          uriParamsJson = write(parsed)
      ))
    } else {
      None
    }
  }
}