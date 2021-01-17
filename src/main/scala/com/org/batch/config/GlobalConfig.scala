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
  var csvDelimiter: Option[String] = None
  var header: Option[Boolean] = None

  def withMongoInputUri(mongoInputUri: Option[String]): GlobalConfigBuilder = {
    this.mongoInputUri = mongoInputUri
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

  def withDelimiter(delimiter: Option[String]): GlobalConfigBuilder = {
    this.csvDelimiter = delimiter
    this
  }

  def withHeader(header: Option[Boolean]): GlobalConfigBuilder = {
    this.header = header
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
    instance.csvDelimiter = this.csvDelimiter
    instance.header = this.header

    instance
  }
}

case class GlobalConfig() extends JobConfig {
  var mongoInputUri: Option[String] = None
  var mongoOutputUri: Option[String] = None
  var inputSource: Option[String] = None
  var readerType: Option[String] = None
  var writerType: Option[String] = None
  var transformationType: Option[String] = None
  var csvDelimiter: Option[String] = None
  var header: Option[Boolean] = None
}

class CLIParams {
  def buildCLIParams(args: Seq[String]): GlobalConfig = {
    val parser = new OptionParser[GlobalConfigBuilder]("Batch jobs lib") {
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
      opt[String]("csv-delimiter")
        .action((value, c) => c.withDelimiter(Some(value)))
        .text("Delimiter to use with csv source files. Default is ','")
      opt[Boolean]("has-header")
        .action((value, c) => c.withHeader(Some(value)))
        .text("true if files have header, false otherwise. Default is false")
      opt[String]("writer-type")
        .action((value, c) => c.withWriterType(Some(value)))
        .text(s"Which writer type to use. One of: ${WriterFactory.AllWriters.map(wt => wt.toString).mkString(",")}")
    }

    parser.parse(args, GlobalConfigBuilder()) match {
      case None => throw new Exception("An error occurred while parsing CLI arguments")
      case Some(gcb) => gcb.build()
    }
  }
}
