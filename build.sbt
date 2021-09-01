name := "scala-zio-kafka"
version := "0.1"
scalaVersion := "2.13.6"

libraryDependencies ++= Seq(
  "dev.zio" %% "zio-kafka"   % "0.15.0",
  "dev.zio" %% "zio-json"    % "0.1.5"
)

idePackagePrefix := Some("com.lms")
