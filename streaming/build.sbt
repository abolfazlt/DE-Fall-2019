name := "spark"

version := "0.1"

scalaVersion := "2.11.12"

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case _ => MergeStrategy.first
}

libraryDependencies += ("org.apache.spark" %% "spark-core" % "2.3.2")

libraryDependencies += ("org.apache.spark" %% "spark-sql" % "2.3.2")

libraryDependencies += ("org.apache.spark" %% "spark-streaming" % "2.3.2")

libraryDependencies += "org.apache.spark" %% "spark-streaming-kafka-0-10" % "2.3.2"

libraryDependencies += "org.apache.kafka" % "kafka-clients" % "2.2.1"

mainClass in(Compile, run) := Some("TFIDF")