package com.libertexgroup.clickstream.algebras

import com.libertexgroup.clickstream.config.ProgramConfig
import com.libertexgroup.clickstream.models.messages.{AdjustClickstream, KafkaRecord, UnifiedClickstream}
import com.libertexgroup.clickstream.target.clickhouse.Target._
import zio.console.Console
import zio.{Chunk, Has, Task, ZIO}

object Clickstream {
  def messageToEitherAdjust(record: KafkaRecord): Task[Either[KafkaRecord, UnifiedClickstream]] =
    ZIO {
      AdjustClickstream.processClickstream(record.message) match {
        case Some(value) => Right(UnifiedClickstream.fromAdjustClickstream(value))
        case None => Left(record)
      }
    }

  def messageToEitherAudience(record: KafkaRecord): Task[Either[KafkaRecord, UnifiedClickstream]] =
    ZIO {
      AdjustClickstream.processClickstream(record.message) match {
        case Some(value) => Right(UnifiedClickstream.fromAdjustClickstream(value))
        case None => Left(record)
      }
    }


  val storeChunkToTargets:
    Chunk[Either[KafkaRecord, UnifiedClickstream]] =>
      ZIO[Console with Has[ProgramConfig], Throwable, Unit]  =
    chunk => for {
      _ <- writeClickstream(
        chunk
          .filter(_.isRight)
          .map(_.right.get)
      ) *> writeDLQ(
        chunk
          .filter(_.isLeft)
          .map(_.left.get)
      )
    } yield ()
}
