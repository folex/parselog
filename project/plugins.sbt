import sbt.Resolver

resolvers ++= Seq(
  "Typesafe release repository" at "http://repo.typesafe.com/typesafe/releases/",
  "sonatype-releases" at "https://oss.sonatype.org/content/repositories/releases/",
  Resolver.url(
    "bintray-sbt-plugin-releases",
    url("http://dl.bintray.com/content/sbt/sbt-plugin-releases")
  )(Resolver.ivyStylePatterns)
)

addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "0.11.2")
addSbtPlugin("com.orrsella" % "sbt-sublime" % "1.1.0")
