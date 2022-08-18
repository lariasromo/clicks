package com.libertexgroup.clickstream.target.greenplum

import zio.{ZIO, system}

case class Config(
                            jdbcUrl:String,
                            username:String,
                            password:String,
                            table:String,
                            driver:String
                          ) {

  val tabledlq = s"dlq_$table"
}

object Config {
  def make: ZIO[system.System, Exception, Config] = for {
    jdbcUrl <- system.env("GREENPLUM_JDBC_URL")
    username <- system.env("GREENPLUM_USERNAME")
    password <- system.env("GREENPLUM_PASSWORD")
    table <- system.env("GREENPLUM_TABLE")
    driver <- system.env("GREENPLUM_DRIVER")
  } yield Config(
    jdbcUrl = jdbcUrl.getOrElse(""),
    username = username.getOrElse(""),
    password = password.getOrElse(""),
    table = table.getOrElse(""),
    driver = driver.getOrElse("org.postgresql.Driver"),
  )
}