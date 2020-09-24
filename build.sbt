name := "DataFrameDatasetTutorial2020"
version := "1.0"
scalaVersion := "2.12.10"
resolvers += "Typesafe Releases" at
"http://repo.typesafe.com/typesafe/releases/"
libraryDependencies += "log4j" % "log4j" % "1.2.17"


libraryDependencies += "org.apache.spark" %% "spark-core" % "2.4.6"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.6"

libraryDependencies += "org.apache.spark" %% "spark-streaming" % "2.4.6"

libraryDependencies += "org.apache.spark" %% "spark-graphx" % "2.4.6"

libraryDependencies += "org.apache.spark" %% "spark-mllib" % "2.4.6"
libraryDependencies += "org.plotly-scala" %% "plotly-core" % "0.7.6"
libraryDependencies += "org.plotly-scala" %% "plotly-render" % "0.7.6"
fork in run := true

