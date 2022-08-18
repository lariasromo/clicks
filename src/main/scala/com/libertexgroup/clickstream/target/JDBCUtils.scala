package com.libertexgroup.clickstream.target

import zio.{ZIO, ZManaged}

import java.sql.{Connection, DriverManager}

object JDBCUtils {
  def getConnectionResource(
                             JDBC_DRIVER: String,
                             DB_URL: String,
                             USER: String,
                             PASS: String): ZManaged[Any, Throwable, Connection] =
    ZManaged.make(
      ZIO {
        try {
          Class.forName(JDBC_DRIVER)
          DriverManager.getConnection(DB_URL, USER, PASS)
        } catch {
          case e: Exception => {
            e.printStackTrace()
            null
          }
        }
      }
    )(c => ZIO.effectTotal(c.close()))
}
