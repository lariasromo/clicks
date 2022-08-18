package com.libertexgroup.clickstream.config

import zio.duration.{Duration, durationInt}
import zio.{ZIO, system}

import scala.util.Try

object KafkaConfig {

  case class Config(
                    kafkaAudienceTopic: String,
                    kafkaAdjustTopic: String,
                    kafkaBrokers: List[String],
                    consumerGroup: String,
                    flushSeconds: Duration,
                    batchSize: Int
                   )

  def make: ZIO[system.System, SecurityException, Config] = for {
      kafkaBrokers <- system.envOrElse("KAFKA_BROKERS", throw new Exception("Variable KAFKA_BROKERS should be set"))
      consumerGroup <- system.envOrElse("CONSUMER_GROUP", throw new Exception("Variable CONSUMER_GROUP should be set"))
      kafkaAudienceTopic <- system.envOrElse("AUDIENCE_TOPIC", throw new Exception("Variable AUDIENCE_TOPIC should be set"))
      kafkaAdjustTopic <- system.envOrElse("ADJUST_TOPIC", throw new Exception("Variable ADJUST_TOPIC should be set"))
      flushSeconds <- system.env("FLUSH_SECONDS")
      batchSize <- system.env("BATCH_SIZE")
    } yield Config(
        kafkaAudienceTopic,
        kafkaAdjustTopic,
        kafkaBrokers.split(",").toList,
        consumerGroup,
        Try(flushSeconds.map(_.toInt)).toOption.flatten.getOrElse(300).seconds,
        Try(batchSize.map(_.toInt)).toOption.flatten.getOrElse(1000)
      )
}
