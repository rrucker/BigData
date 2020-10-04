name := "Spark301"

version := "0.1"

scalaVersion := "2.12.10"
libraryDependencies += "log4j" % "log4j" % "1.2.17"
libraryDependencies += "org.apache.spark" %% "spark-core" % "3.0.1"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.0.1"
libraryDependencies += "org.apache.spark" %% "spark-streaming" % "3.0.1"
libraryDependencies += "org.apache.spark" %% "spark-graphx" % "3.0.1"
libraryDependencies += "org.apache.spark" %% "spark-mllib" % "3.0.1"
libraryDependencies += "org.plotly-scala" %% "plotly-core" % "0.7.6"
libraryDependencies += "org.plotly-scala" %% "plotly-render" % "0.7.6"
fork in run := true
