package com.libertexgroup.clickstream.models.messages

import zio.console.{Console, putStrLn}
import zio.{Has, ULayer, ZIO, ZLayer, system}

case class DailyPartitionsConfig (
  tableName:String,
  bucketName:String,
  rollingWindowLengthDays:Int,
  jdbcUrl:String,
  username:String,
  password:String
)


object DailyPartitionsConfig {
  def live: ZIO[system.System with Console, Throwable, ULayer[Has[DailyPartitionsConfig]]] = for {
    tableName <- system.env("TABLE_NAME")
      .someOrFail(new Exception("TABLE_NAME environment variable is missing"))
    bucketName <- system.env("BUCKET_NAME")
      .someOrFail(new Exception("BUCKET_NAME environment variable is missing"))
    rollingWindowLengthDays <- system.env("ROLLING_WINDOW_LENGTH_DAYS")
      .someOrFail(new Exception("ROLLING_WINDOW_LENGTH_DAYS environment variable is missing"))
    jdbcUrl <- system.env("JDBC_URL")
      .someOrFail(new Exception("JDBC_URL environment variable is missing"))
    username <- system.env("USERNAME")
      .someOrFail(new Exception("USERNAME environment variable is missing"))
    password <- system.env("PASSWORD")
      .someOrFail(new Exception("PASSWORD environment variable is missing"))
    config <- ZIO(
      DailyPartitionsConfig(
        tableName=tableName,
        bucketName=bucketName,
        rollingWindowLengthDays=rollingWindowLengthDays.toInt,
        jdbcUrl=jdbcUrl,
        username=username,
        password=password,
      )
    )
    _ <- putStrLn("Program Config...")
    _ <- putStrLn(config.toString)
  } yield ZLayer.succeed( config )

}