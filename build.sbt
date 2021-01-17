name := "batch-job-lib"

version := "0.1"

scalaVersion := "2.11.12"

artifactName := { (sv: ScalaVersion, module: ModuleID, artifact: Artifact) =>
  artifact.name + "_" + sv.binary + "-2.4.6" + "_" + module.revision + "." + artifact.extension
}

libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.6"
libraryDependencies += "com.github.scopt" %% "scopt" % "4.0.0"
libraryDependencies += "org.apache.logging.log4j" %% "log4j-api-scala" % "11.0"
libraryDependencies += "org.mongodb.spark" %% "mongo-spark-connector" % "2.2.8"
libraryDependencies += "io.circe" %% "circe-core" % "0.11.2"
libraryDependencies += "io.circe" %% "circe-generic" % "0.11.2"
libraryDependencies += "io.circe" %% "circe-parser" % "0.11.2"
libraryDependencies += "io.circe" %% "circe-optics" % "0.11.0"
libraryDependencies += "org.scala-lang" % "scala-reflect" % "2.11.12"

// Testing
libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.1" % Test
libraryDependencies += "org.scalactic" %% "scalactic" % "3.2.1"
libraryDependencies += "org.scalatestplus" %% "mockito-3-4" % "3.2.2.0" % "test"

test in assembly := {}

mainClass in assembly := Some("com.org.challenge.stream.AppLibrary")

assemblyMergeStrategy in assembly := {
  case x if Assembly.isConfigFile(x) =>
    MergeStrategy.concat
  case PathList(ps @ _*) if Assembly.isReadme(ps.last) || Assembly.isLicenseFile(ps.last) =>
    MergeStrategy.rename
  case PathList("META-INF", xs @ _*) =>
    (xs map {_.toLowerCase}) match {
      case ("manifest.mf" :: Nil) | ("index.list" :: Nil) | ("dependencies" :: Nil) =>
        MergeStrategy.discard
      case ps @ (x :: xs) if ps.last.endsWith(".sf") || ps.last.endsWith(".dsa") =>
        MergeStrategy.discard
      case "plexus" :: xs =>
        MergeStrategy.discard
      case "services" :: xs =>
        MergeStrategy.filterDistinctLines
      case ("spring.schemas" :: Nil) | ("spring.handlers" :: Nil) =>
        MergeStrategy.filterDistinctLines
      case _ => MergeStrategy.first
    }
  case _ => MergeStrategy.first
}