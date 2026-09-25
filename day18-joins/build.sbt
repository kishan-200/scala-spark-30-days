ThisBuild / scalaVersion := "2.12.18"

lazy val root = (project in file("."))
  .settings(
    name := "day18-joins",

    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % "3.5.3",
      "org.apache.spark" %% "spark-sql"  % "3.5.3"
    ),

    fork := true,

    javaOptions ++= Seq(
      "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED"
    )
  )
