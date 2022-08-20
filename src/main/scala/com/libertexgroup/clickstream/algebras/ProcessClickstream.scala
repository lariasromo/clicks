package com.libertexgroup.clickstream.algebras

import com.libertexgroup.clickstream.config.ProgramConfig
import com.libertexgroup.clickstream.models.messages.{KafkaRecord, UnifiedClickstream}
import zio.clock.Clock
import zio.console.Console
import zio.kafka.consumer.{Consumer, Subscription}
import zio.kafka.serde.Serde
import zio.stream.ZStream
import zio.{Chunk, Has, ZIO}

object ProcessClickstream {
  def createStream(topic:String):
  ZIO[Has[ProgramConfig], Throwable, ZStream[Any with Consumer with Clock, Throwable, Chunk[KafkaRecord]]] = for {
    kafkaConfig <- ZIO.access[Has[ProgramConfig]](_.get.kafkaConfig)
    stream <- ZIO {
      Consumer.subscribeAnd( Subscription.topics(topic) )
        .plainStream(Serde.string, Serde.string)
        .zipWithIndex
        .tap { batch => batch._1.offset.commit }
        .map(record => KafkaRecord(topic, record._1.value, record._1.timestamp))
        .groupedWithin(kafkaConfig.batchSize, kafkaConfig.flushSeconds)
    }
  } yield (stream)

  def processAdjust: ZIO[Has[ProgramConfig], Throwable,
    ZStream[Any with Consumer with Clock, Throwable, Chunk[Either[KafkaRecord, UnifiedClickstream]]]] = for {
    kafkaConfig <- ZIO.access[Has[ProgramConfig]](_.get.kafkaConfig)
    stream <- createStream(kafkaConfig.kafkaAdjustTopic)
  } yield stream.mapM { chunk => {
    chunk
      .filter(r => !r.message.contains("activity_kind=impression"))
      .filter(r => !r.message.contains("activity_kind=san_impression"))
      .mapM {
        Clickstream.messageToEitherAdjust
      }
  } }

  def processAudience: ZIO[Has[ProgramConfig], Throwable,
    ZStream[Any with Consumer with Clock, Throwable, Chunk[Either[KafkaRecord, UnifiedClickstream]]]] = for {
    kafkaConfig <- ZIO.access[Has[ProgramConfig]](_.get.kafkaConfig)
    stream <- createStream(kafkaConfig.kafkaAudienceTopic)
  } yield stream.mapM { chunk => chunk.mapM(Clickstream.messageToEitherAudience) }


  def streamingProgram: ZIO[Console with Has[ProgramConfig] with Any with Consumer with Clock, Throwable, Unit] = for {
    streamAdjust <- processAdjust
    streamAudience <- processAudience
    _ <- (streamAdjust ++ streamAudience)
      .tap(Clickstream.storeChunkToTargets)
      .runCollect
      .orDie
  } yield ()
}
