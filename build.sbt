name := """parselog"""

version := "1.0"

scalaVersion := "2.11.7"

// Change this to another test framework if you prefer
libraryDependencies ++= Seq(
  "org.scalatest" %% "scalatest" % "2.2.4" % "test",
  "com.google.re2j" % "re2j" % "1.0",
  "org.xerial" % "sqlite-jdbc" % "3.7.2",
  "org.scalikejdbc" %% "scalikejdbc" % "2.2.7",
  "ch.qos.logback"  %  "logback-classic"   % "1.1.3"
)

javaOptions := Seq("-Xmx2g")

