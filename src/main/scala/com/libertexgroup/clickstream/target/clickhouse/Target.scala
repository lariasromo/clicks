package com.libertexgroup.clickstream.target.clickhouse

import com.libertexgroup.clickstream.config.ProgramConfig
import com.libertexgroup.clickstream.models.messages.{KafkaRecord, UnifiedClickstream}
import com.libertexgroup.clickstream.target.JDBCUtils.getConnectionResource
import zio.console.{Console, putStrLn}
import zio.{Chunk, Has, ZIO}

import java.time.{LocalDate, LocalDateTime}

object Target {
  def writeClickstream(dataset: Chunk[UnifiedClickstream]): ZIO[Console with Has[ProgramConfig], Throwable, Unit] =
    for {
    config <- ZIO.access[Has[ProgramConfig]](_.get)
    c <- if(!dataset.isEmpty) {
      putStrLn("Got non empty clickstream batch of: " + dataset.size) *>
      getConnectionResource(
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
              "(?, ?, ?, ?, ?,    ?, ?, ?, ?, ?,    ?, ?, ?)"
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
              statement.setLong(10, r.createdAt.getTime)
              statement.setString(11, r.origin)
              statement.setString(12, r.uriParamsJson)
              statement.setLong(13, System.currentTimeMillis())
              statement.addBatch()
            })
            statement.executeBatch()
          })

          statement.close()
        })
      } else ZIO.unit
  } yield (c)

  def writeDLQ(dataset: Chunk[KafkaRecord]): ZIO[Console with Has[ProgramConfig], Throwable, Unit] = for {
    config <- ZIO.access[Has[ProgramConfig]](_.get)
    c <- if(!dataset.isEmpty) {
      putStrLn("Got non empty dlq batch of: " + dataset.size) *>
      getConnectionResource(
        config.clickhouseConfig.driver,
        config.clickhouseConfig.jdbcUrl,
        config.clickhouseConfig.username,
        config.clickhouseConfig.password)
        .use(conn => ZIO {
          val statement = conn.prepareStatement(
            "INSERT INTO " + config.clickhouseConfig.dbtableDLQ +
              "(topic, value, createdDate) VALUES " +
              "(?, ?, ?)")

          dataset.split(config.clickhouseConfig.batchSize).foreach(chunks => {
            chunks.foreach(r => {
              statement.setString(1, r.topic)
              statement.setString(2, r.message)
              statement.setLong(3, System.currentTimeMillis())
              statement.addBatch()
            })
            statement.executeBatch()
          })

          statement.close()
        })
    } else ZIO.unit
  } yield (c)


}
