lazy val commonSettings = Seq(
  organization := "com.ibm.poc",
  version := "0.1.0",
  scalaVersion := "2.10.4",
  libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % "1.4.1",
      "org.apache.spark" %% "spark-sql"  % "1.4.1",
      "org.apache.spark" %% "spark-hive"  % "1.4.1",
      "org.apache.mahout" % "mahout-math" % "0.10.2",
      "org.apache.mahout" % "mahout-hdfs" % "0.10.2",
      "org.apache.mahout" % "mahout-mr" % "0.10.2",
      "org.apache.mahout" % "mahout-math-scala_2.10" % "0.10.2"
    ),
  resolvers += "Local Maven Repository" at "file://"+Path.userHome.absolutePath+"/.m2/repository"
)

lazy val root = (project in file(".")).
  settings(commonSettings: _*).
  settings(
    name := "dataupdater"
  )

val buildSettings = Defaults.defaultSettings ++ Seq(
   //…
   javaOptions += "-Xms512M -Xmx2G -Xss8M -XX:+CMSClassUnloadingEnabled -XX:+UseConcMarkSweepGC -XX:MaxPermSize=1024M"
   //…
)