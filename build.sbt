name := "KafkaStreams"

version := "0.1"

scalaVersion := "2.11.12"
val sparkVersion = "2.3.0"

libraryDependencies ++= Seq(
  "org.apache.bahir" %% "spark-streaming-twitter" % sparkVersion,
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" % "spark-sql_2.11" % sparkVersion,
  "org.apache.spark" %% "spark-streaming" % sparkVersion,
  "org.apache.kafka" % "kafka_2.11" % "1.1.1"
)

libraryDependencies ++= Seq("org.apache.spark" % "spark-sql_2.11" % sparkVersion,
  "org.apache.spark" % "spark-sql-kafka-0-10_2.11" % sparkVersion,
  "org.apache.kafka" % "kafka-clients" % "0.11.0.1")

dependencyOverrides += "com.fasterxml.jackson.core" % "jackson-core" % "2.8.7"
dependencyOverrides += "com.fasterxml.jackson.core" % "jackson-databind" % "2.8.7"
dependencyOverrides += "com.fasterxml.jackson.module" % "jackson-module-scala_2.11" % "2.8.7"