package com.libertexgroup.clickstream.config

import com.libertexgroup.clickstream.target.clickhouse
import zio.console.{Console, putStrLn}
import zio.{Has, ULayer, ZIO, ZLayer, system}

case class ProgramConfig (
                           kafkaConfig: KafkaConfig.Config,
                           clickhouseConfig: clickhouse.Config
                         )

object ProgramConfig {
  def live: ZIO[system.System with Console, Throwable, ULayer[Has[ProgramConfig]]] = for {
    kafkaConfig <- KafkaConfig.make
    clickhouseConfig <- clickhouse.Config.make
    config <- ZIO(
      ProgramConfig(
        kafkaConfig = kafkaConfig,
        clickhouseConfig = clickhouseConfig,
      )
    )
    _ <- putStrLn("Program Config...")
    _ <- putStrLn(config.toString)
  } yield ZLayer.succeed( config )

}