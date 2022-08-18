package com.libertexgroup.clickstream.target.greenplum

import com.libertexgroup.clickstream.config.ProgramConfig
import com.libertexgroup.clickstream.models.messages.{KafkaRecord, UnifiedClickstream}
import com.libertexgroup.clickstream.target.JDBCUtils.getConnectionResource
import zio.{Chunk, Has, ZIO}

import java.sql.PreparedStatement

object Target {
  def writeClickstream(dataset: Chunk[UnifiedClickstream]): Unit = for {
    config <- ZIO.access[Has[ProgramConfig]](_.get)
    _ <- getConnectionResource(
      config.greenplumConfig.driver,
      config.greenplumConfig.jdbcUrl,
      config.greenplumConfig.username,
      config.greenplumConfig.password )
      .use( conn => ZIO {
        // TODO: reimplement greenplum
      })
  } yield ()


  def writeDLQ(dataset: Chunk[KafkaRecord]): Unit = for {
    config <- ZIO.access[Has[ProgramConfig]](_.get)
    _ <- getConnectionResource(
                                config.greenplumConfig.driver,
                                config.greenplumConfig.jdbcUrl,
                                config.greenplumConfig.username,
                                config.greenplumConfig.password )
      .use( conn => ZIO {
        // TODO: reimplement greenplum
    })
  } yield ()
}
