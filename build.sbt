ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.12"

lazy val root = (project in file("."))
  .settings(
    name := "clickstreamPipelineZIO"
  )

libraryDependencies ++= Seq(
  "dev.zio" %% "zio-kafka"   % "0.15.0",
  "org.json4s" %% "json4s-native" % "4.1.0-M1",
  "org.apache.httpcomponents" % "httpclient" % "4.5.13",
  "com.clickhouse" % "clickhouse-jdbc" % "0.3.2-patch11"
)

// Credentials to get access to Libertex Artifactory maven repositories
credentials += Credentials(Path.userHome / ".sbt" / "artifactory_credentials")

// Libertex Artifactory repository resolver
resolvers += "Artifactory Realm" at s"https://artifactory.fxclub.org/artifactory/alexandria-release"
resolvers += "Artifactory Realm snapshot" at s"https://artifactory.fxclub.org/artifactory/alexandria-snapshot"
resolvers += "Sonatype OSS Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots"