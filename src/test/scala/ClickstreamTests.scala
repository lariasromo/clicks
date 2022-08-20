import ClickhouseContainerService.Clickhouse
import com.libertexgroup.clickstream.algebras.Clickstream
import com.libertexgroup.clickstream.config.ProgramConfig
import com.libertexgroup.clickstream.models.TargetTypes
import com.libertexgroup.clickstream.models.messages.{KafkaRecord, UnifiedClickstream}
import com.libertexgroup.clickstream.target.JDBCUtils.getConnectionResource
import zio.blocking.Blocking
import zio.clock.Clock
import zio.console.{Console, putStrLn}
import zio.duration.durationInt
import zio.stream.ZStream
import zio.test.Assertion.{isEmpty, isTrue}
import zio.test._
import zio.{Has, Task, ULayer, ZIO, ZLayer, ZManaged, system}
import com.libertexgroup.clickstream.target.clickhouse

import scala.io.Source

object ClickstreamTests extends DefaultRunnableSpec {
  val prgLayer = Clock.live ++ system.System.live ++ Console.live ++ Blocking.live ++
    ClickhouseContainerService.clickhouse()

  def programConfigLayer: ZIO[Clickhouse, Throwable, ULayer[Has[ProgramConfig]]] = for {
    container <- ZIO.access[Clickhouse](_.get)
    configLayer <- ZIO {
      ZLayer.succeed(ProgramConfig(
        kafkaConfig = null,
        clickhouseConfig = clickhouse.Config(
          container.host,
          container.mappedPort(8123),
          "default",
          "clickstream",
          container.username,
          container.password,
          "com.clickhouse.jdbc.ClickHouseDriver", 100
        ),
      ))
    }
  } yield (configLayer)


  object Messages {
    def parseFile(fileName: String): ZStream[Any, Throwable, String] = {
      zio.stream.Stream.fromIteratorManaged(
        ZManaged
          .fromAutoCloseable(
            Task(Source.fromURL(getClass.getResource(fileName)))
          )
          .map(_.getLines())
      )

    }
  }

  def executeQuery(query: String): ZIO[Clickhouse, Throwable, Int] = for {
    container <- ZIO.access[Clickhouse](_.get)
    count <- getConnectionResource(
      "com.clickhouse.jdbc.ClickHouseDriver",
      s"${container.jdbcUrl}",
      container.username, container.password )
      .use( conn => ZIO {
        val statement = conn.createStatement()
        val resultSet = statement.executeQuery(query)
        val count = if(resultSet != null){
          resultSet.next()
          resultSet.getInt(1)
        } else 0
        statement.close()
        count
      })
  } yield (count)

  val adjustMessages: ZStream[Any, Throwable, Either[KafkaRecord, UnifiedClickstream]] =
    Messages.parseFile("adjust_messages.txt")
    .map(r => KafkaRecord("adjust", r, System.currentTimeMillis()))
    .mapM(Clickstream.messageToEitherAdjust)

  val audienceMessages: ZStream[Any, Throwable, Either[KafkaRecord, UnifiedClickstream]] =
    Messages.parseFile("audience_messages.txt")
    .map(r => KafkaRecord("audience", r, System.currentTimeMillis()))
    .mapM(Clickstream.messageToEitherAudience)

  val dlqMessages: ZStream[Any, Throwable, Either[KafkaRecord, UnifiedClickstream]] =
    Messages.parseFile("dlq_messages.txt")
    .map(r => KafkaRecord("sometopic", r, System.currentTimeMillis()))
    .mapM(Clickstream.messageToEitherAudience)

  override def spec: ZSpec[Environment, Failure] = suite("ClickstreamTests")(
    testM("All adjust messages are parsed correctly") {
      val result = for {
        stream <- adjustMessages.runCollect
      } yield(stream.filter(_.isLeft))
      assertM(result)(isEmpty)
    },
    testM("All audience messages are parsed correctly") {
      val result = for {
        stream <- audienceMessages.runCollect
      } yield(stream.filter(_.isLeft))
      assertM(result)(isEmpty)
    },
    testM("DLQ messages are generated") {
      val result = for {
        stream <- dlqMessages.runCollect
      } yield(stream.filter(_.isRight))
      assertM(result)(isEmpty)
    },
    testM("CLickstream and DLQ messages should be inserted") {
      assertM({
        for {
          layer <- programConfigLayer
          sql <- Messages.parseFile("clickstream_table.sql").runCollect
          sql2 <- Messages.parseFile("dlq_clickstream_table.sql").runCollect
          _ <- executeQuery(sql.mkString(" "))
          _ <- executeQuery(sql2.mkString(" "))
          dlqCount <- (audienceMessages ++ adjustMessages ++ dlqMessages).filter(_.isLeft).runCount
          clickCount <- (audienceMessages ++ adjustMessages ++ dlqMessages).filter(_.isRight).runCount
          _ <- (audienceMessages ++ adjustMessages ++ dlqMessages)
            .groupedWithin(100, 1.seconds)
            .mapM(Clickstream.storeChunkToTargets)
            .runCount
            .orDie
            .provideLayer(prgLayer ++ layer)

          realCountClicks <- executeQuery(s"SELECT count(1) from clickstream")
          realCountDLQ <- executeQuery(s"SELECT count(1) from dlq_clickstream")
        } yield ((clickCount == realCountClicks) && (dlqCount == realCountDLQ))
      }.provideLayer(prgLayer))(isTrue)
    },
  )
}