val scala3Version = "2.13.0"

lazy val root = project
  .in(file("."))
  .settings(
    name := "sparkETL",
    version := "0.1.0-SNAPSHOT",

    scalaVersion := scala3Version,

    libraryDependencies ++= Seq(
                              "org.scalameta" %% "munit" % "0.7.29" % Test,
                              "org.apache.spark" %% "spark-core" % "3.3.0",
                              "org.apache.spark" %% "spark-sql" % "3.3.0",
                              "org.scala-lang" %% "toolkit" % "0.1.7",
                              "org.apache.httpcomponents" % "httpclient" % "4.5.13"

    )
  )
