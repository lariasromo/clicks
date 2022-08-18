package com.libertexgroup.clickstream.target.clickhouse

import com.libertexgroup.clickstream.config.ProgramConfig
import com.libertexgroup.clickstream.models.messages.{KafkaRecord, UnifiedClickstream}
import com.libertexgroup.clickstream.target.JDBCUtils.getConnectionResource
import zio.{Chunk, Has, ZIO}

object Target {
  def writeClickstream(dataset: Chunk[UnifiedClickstream]): ZIO[Has[ProgramConfig], Throwable, Unit] = for {
    config <- ZIO.access[Has[ProgramConfig]](_.get)
    c <- getConnectionResource(
      config.clickhouseConfig.driver,
      config.clickhouseConfig.jdbcUrl,
      config.clickhouseConfig.username,
      config.clickhouseConfig.password )
      .use( conn => ZIO {
        val statement = conn.prepareStatement(
          "INSERT INTO " + config.clickhouseConfig.dbtable +
            "(clientId, eventType, country, broker,      sessionId, " +
            "visitorId, gpsAdid,   idfa,    trackerType, createdAt, " +
            "origin,    uriParamsJson, insertedAt) VALUES " +
            "(?, ?, ?, ?, ?," +
            "?, ?, ?, ?, ?," +
            "?, ?, now())"
        )

        dataset.split(config.clickhouseConfig.batchSize).foreach(chunks => {
          chunks.foreach(r => {
            statement.setString(1, r.clientId.getOrElse(""))
            statement.setString(2, r.eventType.getOrElse(""))
            statement.setString(3, r.country.getOrElse(""))
            statement.setString(4, r.broker.getOrElse(""))
            statement.setString(5, r.sessionId.getOrElse(""))
            statement.setString(6, r.visitorId.getOrElse(""))
            statement.setString(7, r.gpsAdid.getOrElse(""))
            statement.setString(8, r.idfa.getOrElse(""))
            statement.setString(9, r.trackerType)
            statement.setTimestamp(10, r.createdAt)
            statement.setString(11, r.origin)
            statement.setObject(12, r.uriParamsJson)
            statement.addBatch()
          })
          statement.executeBatch()
        })

        statement.close()
      })
  } yield (c)

  def writeDLQ(dataset: Chunk[KafkaRecord]): ZIO[Has[ProgramConfig], Throwable, Unit] = for {
    config <- ZIO.access[Has[ProgramConfig]](_.get)
    c <- getConnectionResource(
      config.clickhouseConfig.driver,
      config.clickhouseConfig.jdbcUrl,
      config.clickhouseConfig.username,
      config.clickhouseConfig.password )
      .use( conn => ZIO {
        val statement = conn.prepareStatement(
          "INSERT INTO " + config.clickhouseConfig.dbtableDLQ +
            "(topic, value, createdDate) VALUES " +
            "(?, ?, now())")

        dataset.split(config.clickhouseConfig.batchSize).foreach(chunks => {
          chunks.foreach(r => {
            statement.setString(1, r.topic)
            statement.setString(2, r.message)
            statement.addBatch()
          })
          statement.executeBatch()
        })

        statement.close()
      })
  } yield (c)


}
