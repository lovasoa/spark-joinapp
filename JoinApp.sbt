name := "JoinApp"

version := "1.0"

scalaVersion := "2.11.8"

resolvers += "jitpack" at "https://jitpack.io"

libraryDependencies ++= Seq(
    "org.apache.spark" %% "spark-core" % "2.1.0",
    "org.apache.spark" %% "spark-sql" % "2.1.0",
    "com.github.lovasoa" % "BloomFilter" % "2.2.1"
)
