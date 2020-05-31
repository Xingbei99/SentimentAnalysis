name := "sentiment_analysis"

version := "0.1"

scalaVersion := "2.11.12"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-streaming" % "2.3.1",
  "org.apache.bahir" %% "spark-streaming-twitter" % "2.3.3",
  "org.twitter4j" % "twitter4j-core" % "4.0.4",
  "org.apache.spark" %% "spark-mllib" % "2.3.1"
)

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}