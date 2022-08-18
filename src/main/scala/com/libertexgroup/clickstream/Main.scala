package com.libertexgroup.clickstream

import com.libertexgroup.clickstream.algebras.ProcessClickstream.streamingProgram
import com.libertexgroup.clickstream.config.ProgramConfig
import zio.blocking.Blocking
import zio.clock.Clock
import zio.console.{Console, putStrLn}
import zio.duration.durationInt
import zio.kafka.consumer.Consumer.{AutoOffsetStrategy, OffsetRetrieval}
import zio.kafka.consumer.{Consumer, ConsumerSettings}
import zio.{ExitCode, Has, URIO, ZIO, ZLayer, system}

object Main extends zio.App {

  def consumerLayer: ZIO[Console with Has[ProgramConfig], Throwable, ZLayer[Clock with Blocking, Throwable, Has[Consumer.Service]]] =
    for {
      config <- ZIO.access[Has[ProgramConfig]](_.get)
      _ <- putStrLn(
        s"Connecting to ${config.kafkaConfig.kafkaBrokers.mkString(",")} " +
          s"with consumer group: ${config.kafkaConfig.consumerGroup} " +
          s"consuming from ${config.kafkaConfig.kafkaAdjustTopic} and  ${config.kafkaConfig.kafkaAudienceTopic}"
      )
      layer <- ZIO {
      Consumer.make (
        ConsumerSettings (config.kafkaConfig.kafkaBrokers)
        .withProperty("session.timeout.ms", Int.box(5*60*1000))
        .withProperty("group.max.session.timeout.ms", Int.box(5*60*1000))
        .withOffsetRetrieval (OffsetRetrieval.Auto (AutoOffsetStrategy.Earliest)) //remove earliest
        .withGroupId (config.kafkaConfig.consumerGroup)
        .withCloseTimeout (30.seconds)

      )
    }
  } yield layer.toLayer


  override def run(args: List[String]): URIO[zio.ZEnv, ExitCode] = {
    (for {
      programConfigLayer <- ProgramConfig.live
      kafkaLayer <- consumerLayer.provideLayer(Console.live ++ programConfigLayer)
      layer <- ZIO( Clock.live ++ system.System.live ++ Console.live ++ kafkaLayer ++ programConfigLayer )

      _ <- streamingProgram.provideLayer(layer)
    } yield ()
      ).orDie.as(ExitCode.success)
  }
}
