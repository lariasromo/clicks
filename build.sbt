ThisBuild / version := "0.2"

ThisBuild / scalaVersion := "2.12.12"

lazy val root = (project in file("."))
  .settings(
    name := "clickstreamPipelineZIO"
  )

libraryDependencies ++= Seq(
  "dev.zio" %% "zio-kafka"   % "0.15.0",
  "org.json4s" %% "json4s-native" % "4.1.0-M1",
  "com.clickhouse" % "clickhouse-jdbc" % "0.3.2-patch11",
  "dev.zio" %% "zio-test"     % "1.0.16" % "test",
  "dev.zio" %% "zio-test-sbt" % "1.0.16" % "test",
  "com.dimafeng" %% "testcontainers-scala-scalatest" % "0.40.10" % "test",
  "com.dimafeng" %% "testcontainers-scala-clickhouse" % "0.40.10" % "test",
)

// Credentials to get access to Libertex Artifactory maven repositories
credentials += Credentials(Path.userHome / ".sbt" / "artifactory_credentials")

// Libertex Artifactory repository resolver
resolvers += "Artifactory Realm" at s"https://artifactory.fxclub.org/artifactory/alexandria-release"
resolvers += "Artifactory Realm snapshot" at s"https://artifactory.fxclub.org/artifactory/alexandria-snapshot"
resolvers += "Sonatype OSS Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots"

enablePlugins(DockerPlugin)
enablePlugins(JavaAppPackaging)

dockerBaseImage := "openjdk:11-jdk"
