name := "claude-spark-etl"

version := "1.0.0"

scalaVersion := "2.12.18"

javacOptions ++= Seq("-source", "11", "-target", "11")

libraryDependencies ++= Seq(
  // Spark Core
  "org.apache.spark" %% "spark-core" % "3.5.6" % "provided",
  "org.apache.spark" %% "spark-sql" % "3.5.6" % "provided",
  "org.apache.spark" %% "spark-streaming" % "3.5.6" % "provided",

  // Spark Connectors
  "org.apache.spark" %% "spark-sql-kafka-0-10" % "3.5.6",
  "org.apache.spark" %% "spark-avro" % "3.5.6",

  // Avro
  "org.apache.avro" % "avro" % "1.11.3",

  // JDBC Drivers
  "org.postgresql" % "postgresql" % "42.7.1",
  "com.mysql" % "mysql-connector-j" % "8.2.0",

  // AWS S3
  "org.apache.hadoop" % "hadoop-aws" % "3.3.6",
  "com.amazonaws" % "aws-java-sdk-bundle" % "1.12.367",

  // JSON Parsing
  "com.typesafe.play" %% "play-json" % "2.9.4",

  // Logging
  "ch.qos.logback" % "logback-classic" % "1.4.14",
  "net.logstash.logback" % "logstash-logback-encoder" % "7.4",

  // Testing
  "org.scalatest" %% "scalatest" % "3.2.17" % Test,
  "org.scalatestplus" %% "mockito-4-11" % "3.2.17.0" % Test
)

// Assembly settings for spark-submit
assembly / assemblyMergeStrategy := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case "reference.conf" => MergeStrategy.concat
  case x => MergeStrategy.first
}

assembly / assemblyJarName := s"${name.value}-${version.value}.jar"

// Test settings
Test / parallelExecution := false
Test / fork := true
Test / javaOptions += "-Xmx2G"

// Scala compiler options
scalacOptions ++= Seq(
  "-deprecation",
  "-feature",
  "-unchecked",
  "-Xlint",
  "-Ywarn-dead-code",
  "-Ywarn-numeric-widen",
  "-Ywarn-value-discard"
)
