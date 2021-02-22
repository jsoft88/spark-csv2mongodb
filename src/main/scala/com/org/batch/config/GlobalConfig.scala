package com.org.batch.config

import com.org.batch.factory.{ReaderFactory, TransformationFactory, WriterFactory}
import scopt.OptionParser

case class GlobalConfigBuilder() {
  var mongoInputUri: Option[String] = None
  var mongoOutputUri: Option[String] = None
  var inputSource: Option[String] = None
  var readerType: Option[String] = None
  var writerType: Option[String] = None
  var transformationType: Option[String] = None
  var readerConfigKey: Option[String] = None
  var appName: Option[String] = None
  var mongoOutputDatabase: Option[String] = None
  var mongoOutputCollection: Option[String] = None

  def withReaderConfigKey(readerConfigKey: Option[String]): GlobalConfigBuilder = {
    this.readerConfigKey = readerConfigKey
    this
  }

  def withMongoInputUri(mongoInputUri: Option[String]): GlobalConfigBuilder = {
    this.mongoInputUri = mongoInputUri
    this
  }

  def withMongoOutputDatabase(mongoOutputDatabase: Option[String]): GlobalConfigBuilder  = {
    this.mongoOutputDatabase = mongoOutputDatabase
    this
  }

  def withMongoOutputCollection(mongoOutputCollection: Option[String]): GlobalConfigBuilder = {
    this.mongoOutputCollection = mongoOutputCollection
    this
  }

  def withMongoOutputUri(mongoOuputUri: Option[String]): GlobalConfigBuilder = {
    this.mongoOutputUri = mongoOuputUri
    this
  }

  def withReaderType(readerType: Option[String]): GlobalConfigBuilder = {
    this.readerType = readerType
    this
  }

  def withTransformationType(transformationType: Option[String]) = {
    this.transformationType = transformationType
    this
  }

  def withWriterType(writerType: Option[String]): GlobalConfigBuilder = {
    this.writerType = writerType
    this
  }

  def withInputSource(inputSource: Option[String]): GlobalConfigBuilder = {
    this.inputSource = inputSource
    this
  }

  def withAppName(appName: Option[String]): GlobalConfigBuilder = {
    this.appName = appName
    this
  }

  def build(): GlobalConfig = {
    val instance = new GlobalConfig
    instance.writerType = this.writerType
    instance.readerType = this.readerType
    instance.transformationType = this.transformationType
    instance.inputSource = this.inputSource
    instance.mongoInputUri = this.mongoInputUri
    instance.mongoOutputUri = this.mongoOutputUri
    instance.readerConfigKey = this.readerConfigKey
    instance.appName = this.appName
    instance.mongoOutputCollection = this.mongoOutputCollection
    instance.mongoOutputDatabase = this.mongoOutputDatabase

    instance
  }
}

case class GlobalConfig() extends JobConfig {
  var mongoInputUri: Option[String] = None
  var mongoOutputUri: Option[String] = None
  var mongoOutputDatabase: Option[String] = None
  var mongoOutputCollection: Option[String] = None
  var inputSource: Option[String] = None
  var readerType: Option[String] = None
  var writerType: Option[String] = None
  var transformationType: Option[String] = None
  var readerConfigKey: Option[String] = None
  var appName: Option[String] = None
}

class CLIParams {
  def buildCLIParams(args: Seq[String]): GlobalConfig = {
    val parser = new OptionParser[GlobalConfigBuilder]("Batch jobs lib") {
      opt[String](name = "app-name")
        .action((value, c) => c.withAppName(Some(value)))
        .text("Name of the application to run. Useful when there are many batch jobs implemented inside the lib")

      opt[String]("reader-type")
        .action((value, c) => c.withReaderType(Some(value)))
        .text(s"Which reader type to use. One of ${ReaderFactory.AllReaders.map(ar => ar.toString).mkString(",")}")

      opt[String]("transformation-type")
        .action((value, c) => c.withTransformationType(Some(value)))
        .text(s"Which transformation type to use. One of ${TransformationFactory.AllTransformations.map(at => at.toString).mkString(",")}")

      opt[String]("mongo-input-uri")
        .action((value, c) => c.withMongoInputUri(Some(value)))
        .text("Value of the mongo input URI")

      opt[String]("mongo-output-uri")
        .action((value, c) => c.withMongoOutputUri(Some(value)))
        .text("Value of the mongo output URI")
      opt[String]("input-source")
        .action((value, c) => c.withInputSource(Some(value)))
        .text("Full path to the input source. Include protocol such as hdfs://, file://, s3://, etc.")
      opt[String]("reader-config-key")
        .action((value, c) => c.withReaderConfigKey(Some(value)))
        .text("Reader config json file under resources/reader_config directory. Make sure to exclude .json part in the value")
      opt[String]("writer-type")
        .action((value, c) => c.withWriterType(Some(value)))
        .text(s"Which writer type to use. One of: ${WriterFactory.AllWriters.map(wt => wt.toString).mkString(",")}")
      opt[String](name = "mongo-output-database")
        .action((value, c) => c.withMongoOutputDatabase(Some(value)))
        .text("The database containing the collection to which the dataframe will be written to")
      opt[String](name = "mongo-output-collection")
        .action((value, c) => c.withMongoOutputCollection(Some(value)))
        .text("The collection to which the dataframe will be written to")
    }

    parser.parse(args, GlobalConfigBuilder()) match {
      case None => throw new Exception("An error occurred while parsing CLI arguments")
      case Some(gcb) => gcb.build()
    }
  }
}
