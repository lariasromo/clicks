package com.libertexgroup.clickstream.target.clickhouse

import zio.system

import scala.util.Try

case class Config(
                            host: String,
                            port: Int,
                            databaseName: String,
                            tableName: String,
                            username: String,
                            password: String,
                            driver: String,
                            batchSize: Int
                            ){
  val jdbcUrl = s"jdbc:clickhouse://$host:$port/$databaseName"
  val dbtable = s"$databaseName.$tableName"
  val dbtableDLQ = s"$databaseName.dlq_$tableName"
}

object Config {
  def make = for {
    batchSize <- system.env("CLICKHOUSE_BATCH_SIZE")
    host <- system.env("CLICKHOUSE_HOST")
    port <- system.env("CLICKHOUSE_PORT")
    databaseName <- system.env("CLICKHOUSE_DATABASE_NAME")
    tableName <- system.env("CLICKHOUSE_TABLE_NAME")
    username <- system.env("CLICKHOUSE_USERNAME")
    password <- system.env("CLICKHOUSE_PASSWORD")
    driver <- system.env("CLICKHOUSE_DRIVER")
  } yield Config(
    batchSize = batchSize.flatMap(p => Try(p.toInt).toOption).getOrElse(1000),
    host = host.getOrElse(""),
    port = port.flatMap(p => Try(p.toInt).toOption).getOrElse(8123),
    databaseName = databaseName.getOrElse(""),
    tableName = tableName.getOrElse(""),
    username = username.getOrElse(""),
    password = password.getOrElse(""),
    driver = driver.getOrElse("com.clickhouse.jdbc.ClickHouseDriver"),
  )
}