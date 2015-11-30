lazy val commonSettings = Seq(
  organization := "io.esse",
  version := "0.1.0",
  scalaVersion := "2.10.4"
)

lazy val root = (project in file(".")).
  settings(commonSettings: _*).
  settings(
    name := "POC Project",

    libraryDependencies += "org.apache.spark" %% "spark-core" % "1.4.1",

    libraryDependencies += "org.apache.spark" %% "spark-sql" % "1.4.1",

    libraryDependencies += "org.apache.spark" %% "spark-hive" % "1.0.0",

    libraryDependencies += "org.apache.spark" %% "spark-streaming" % "1.4.1",

    libraryDependencies += "com.databricks" %% "spark-csv" % "1.0.3",

    libraryDependencies += "com.datastax.spark" %% "spark-cassandra-connector" % "1.4.0-M1"
  )

  sources in (Compile, doc) ~= (_ filter (_.getName endsWith ".scala"))

  excludeFilter in unmanagedSources := HiddenFileFilter || "*.java"
