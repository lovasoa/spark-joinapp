name := "JoinApp"

version := "1.0"

scalaVersion := "2.11.8"

resolvers += "jitpack" at "https://jitpack.io"

libraryDependencies ++= Seq(
    "org.apache.spark" %% "spark-core" % "2.1.0" % "provided",
    "org.apache.spark" %% "spark-sql" % "2.1.0" % "provided",
    "com.github.scopt" %% "scopt" % "3.6.0"
)

assemblyOutputPath in assembly := file("target/joinapp.jar")
scalacOptions ++= Seq("-deprecation", "-feature")
