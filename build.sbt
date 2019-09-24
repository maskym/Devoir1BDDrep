name := "test2"

version := "0.1"

scalaVersion := "2.11.12"

updateOptions := updateOptions.value.withCachedResolution(true)

libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.4"
libraryDependencies += "org.apache.spark" %% "spark-graphx" % "2.4.4"