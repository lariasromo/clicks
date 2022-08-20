import com.dimafeng.testcontainers.ClickHouseContainer
import org.testcontainers.utility.DockerImageName
import zio.blocking.{Blocking, effectBlocking}
import zio._

object ClickhouseContainerService {
  type Clickhouse = Has[ClickHouseContainer]

  def clickhouse(imageName: String = "clickhouse/clickhouse-server"):
    ZLayer[Blocking, Nothing, Clickhouse] =
    ZManaged.make {
      effectBlocking {
        val container = new ClickHouseContainer(DockerImageName.parse("clickhouse/clickhouse-server"))
        container.start()
        container
      }.orDie
    }(container => effectBlocking(container.stop()).orDie).toLayer
}