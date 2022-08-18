package com.libertexgroup.clickstream.config

import com.libertexgroup.clickstream.models.TargetTypes
import com.libertexgroup.clickstream.target.{clickhouse, greenplum}
import zio.console.{Console, putStrLn}
import zio.{Has, ULayer, ZIO, ZLayer, system}

case class ProgramConfig (
                           kafkaConfig: KafkaConfig.Config,
                           clickhouseConfig: clickhouse.Config,
                           greenplumConfig: greenplum.Config,
                           targets:Set[TargetTypes.Value]
                         )

object ProgramConfig {
  def live: ZIO[system.System with Console, Throwable, ULayer[Has[ProgramConfig]]] = for {
    kafkaConfig <- KafkaConfig.make
    clickhouseConfig <- clickhouse.Config.make
    greenplumConfig <- greenplum.Config.make
    target <- system.env("TARGET").someOrFail(new Exception("TARGET environment variable is missing"))
    config <- ZIO(
      ProgramConfig(
        kafkaConfig = kafkaConfig,
        greenplumConfig = greenplumConfig,
        clickhouseConfig = clickhouseConfig,
        targets = target.split(",").map(t => TargetTypes.withName(t.toUpperCase)).toSet
      )
    )
    _ <- putStrLn("Program Config...")
    _ <- putStrLn(config.toString)
  } yield ZLayer.succeed( config )

}