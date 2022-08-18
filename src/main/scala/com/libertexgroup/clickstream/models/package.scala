package com.libertexgroup.clickstream

import org.apache.http.client.utils.URLEncodedUtils

import java.net.URI
import java.nio.charset.Charset
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import scala.jdk.CollectionConverters.collectionAsScalaIterableConverter

import scala.util.{Failure, Success, Try}

package object models {
  def tryParseDate(dateStr:Option[String]) = dateStr match {
    case Some(d) => Try(java.sql.Timestamp.valueOf(d)) match {
      case Success(value) => value
      case Failure(_) => Try({
        java.sql.Timestamp.valueOf(
          LocalDateTime.from(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm").parse(d))
        )
      }) match {
        case Success(value) => value
        case Failure(_) => Try(new java.sql.Timestamp(d.toLong)) match {
          case Success(value) => value
          case Failure(_) => java.sql.Timestamp.valueOf(LocalDateTime.now)
        }
      }
    }
    case None => java.sql.Timestamp.valueOf(LocalDateTime.now)
  }
  def parseQueryParams(uri: String): Map[String, String] = {
    val u = "http://example.com/" + uri
    Try({
      URLEncodedUtils
        .parse(new URI(u), Charset.forName("utf8"))
        .asScala
        .map(pair => pair.getName -> pair.getValue)
        .toMap
        .filter(_._1.nonEmpty)
        .filter(!_._1.isBlank)
    }) match {
      case Success(value) => {
        value
      }
      case Failure(error) => {
        println(error.getMessage)
        println("Failed to parse " + u)
        Map.empty
      }
    }
  }
}
