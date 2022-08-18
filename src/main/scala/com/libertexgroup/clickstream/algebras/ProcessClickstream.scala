package com.libertexgroup.clickstream.algebras

import com.libertexgroup.clickstream.config.ProgramConfig
import com.libertexgroup.clickstream.models.TargetTypes._
import com.libertexgroup.clickstream.models.messages.{AdjustClickstream, AudienceClickstream, KafkaRecord, UnifiedClickstream}
import com.libertexgroup.clickstream.target.{clickhouse, greenplum}
import zio.clock.Clock
import zio.console.Console
import zio.console.Console.Service.live.putStrLn
import zio.{Chunk, Has, ZIO}
import zio.duration.{Duration, durationInt}
import zio.kafka.consumer.{Consumer, Subscription}
import zio.kafka.serde.Serde
import zio.stream.ZStream

object ProcessClickstream {

  def processAdjust(topic:String, batchSize: Int, flushSeconds: Duration):
  ZStream[Any with Consumer with Clock, Throwable, Chunk[Either[KafkaRecord, UnifiedClickstream]]] = {
    Consumer.subscribeAnd( Subscription.topics(topic) )
      .plainStream(Serde.string, Serde.string)
      .zipWithIndex
      .tap { batch => batch._1.offset.commit }
      .map(record => KafkaRecord(topic, record._1.value))
      .groupedWithin(batchSize, flushSeconds)
      .map { chunk => {
        chunk
          .filter(r => !r.message.contains("activity_kind=impression"))
          .filter(r => !r.message.contains("activity_kind=san_impression"))
          .map( record => AdjustClickstream.processClickstream(record.message) match {
            case Some(value) => Right(UnifiedClickstream.fromAdjustClickstream(value))
            case None => Left(record)
          } )
      } }
  }

  def processAudience(topic:String, batchSize: Int, flushSeconds: Duration):
  ZStream[Any with Consumer with Clock, Throwable, Chunk[Either[KafkaRecord, UnifiedClickstream]]] = {
    Consumer.subscribeAnd( Subscription.topics(topic) )
      .plainStream(Serde.string, Serde.string)
      .zipWithIndex
      .tap { batch => batch._1.offset.commit }
      .map(record => KafkaRecord(topic, record._1.value))
      .groupedWithin(batchSize, flushSeconds)
      .map { chunk => chunk.map( record => AudienceClickstream.processClickstream(record.message) match {
        case Some(value) => Right(UnifiedClickstream.fromAudienceClickstream(value))
        case None => Left(record)
      } ) }


  }

  def streamingProgram: ZIO[Any with Console with Consumer with Clock with Has[ProgramConfig], Throwable, Unit] = for {
    config <- ZIO.access[Has[ProgramConfig]](_.get)
    stream <- ZIO {
      processAdjust(
        config.kafkaConfig.kafkaAdjustTopic,
        config.kafkaConfig.batchSize,
        config.kafkaConfig.flushSeconds
      ) ++ processAudience(
        config.kafkaConfig.kafkaAudienceTopic,
        config.kafkaConfig.batchSize,
        config.kafkaConfig.flushSeconds
      )
    }
    _ <- stream
      .tap{ _.mapM {
        case Left(value) => putStrLn("Got invalid record: " + value.message)
        case Right(value) => putStrLn(
          s"Got valid ${value.origin} record " +
            s"of type ${value.eventType.getOrElse("unknown")}"
        )
      }}
      .tap(chunk => {
      val dlqChunk: Chunk[KafkaRecord] = chunk.filter(_.isLeft).map(_.left.get)
      val clickstreamChunk: Chunk[UnifiedClickstream] = chunk.filter(_.isRight).map(_.right.get)
      (if (config.targets contains GREENPLUM) {
//        greenplum.Target.writeClickstream(clickstreamChunk) ++ greenplum.Target.writeDLQ(dlqChunk)
        ZIO.unit
      } else ZIO.unit) *>
      (if (config.targets contains CLICKHOUSE) {
        clickhouse.Target.writeClickstream(clickstreamChunk) *> clickhouse.Target.writeDLQ(dlqChunk)
      } else ZIO.unit)
    })
      .runCollect
      .orDie
  } yield ()
}
