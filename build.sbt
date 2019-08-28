name := "Purchases"

version := "0.1"

scalaVersion := "2.11.11"

resolvers ++= Seq(
  Resolver.jcenterRepo
)

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}

libraryDependencies += "mysql" % "mysql-connector-java" % "5.1.16"
libraryDependencies += "net.andreinc.mockneat" % "mockneat" % "0.1.7"
libraryDependencies += "org.apache.spark" %% "spark-core" % "2.4.3" % "provided"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.3" % "provided"