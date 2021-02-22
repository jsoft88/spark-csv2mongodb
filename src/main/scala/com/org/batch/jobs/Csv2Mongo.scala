package com.org.batch.jobs

import com.org.batch.config.files.Parser
import com.org.batch.config.{GlobalConfig, JobConfig}
import com.org.batch.core.SparkJob
import com.org.batch.factory.{JobFactory, ReaderFactory, TransformationFactory, WriterFactory}
import com.org.batch.schemas.{SchemaManager, SchemaManagerParser, SchemaManagerReader}
import org.apache.spark.sql.{DataFrame, SparkSession}

class Csv2Mongo[+T <: JobConfig](config: T) extends SparkJob(config) {
  private var sparkSession: SparkSession = _
  private var readerType: String = ""
  private var transformationType: String = ""
  private var writerType: String = ""
  private val globalConfig = config.asInstanceOf[GlobalConfig]
  private var sourceConfig: Parser = _
  private var schemaManager: SchemaManager = _
  private var schemaManagerReader: SchemaManager = _

  override protected def setupJob(): Unit = {
    globalConfig.readerType match {
      case None => throw new Exception("Expected reader type to be present but None found")
      case Some(rt) => this.readerType = rt
    }
    globalConfig.transformationType match {
      case None => throw new Exception("Expected transformation type to be present but None found")
      case Some(tt) => this.transformationType = tt
    }
    globalConfig.writerType match {
      case None => throw new Exception("Expected writer type to be present but None found")
      case Some(wt) => this.writerType = wt
    }

    globalConfig.readerConfigKey match {
      case None => throw new Exception("Missing configuration key for config file under resources/reader_config")
      case Some(sc) => {
        this.sourceConfig = new Parser(sc)
        this.schemaManager = new SchemaManager(sc, SchemaManagerParser)
        this.schemaManagerReader = new SchemaManager(sc, SchemaManagerReader)
      }
    }

    this.sparkSession = SparkSession.builder().appName(s"${JobFactory.Csv2Mongo.toString}").getOrCreate()
  }

  override protected def read(): Option[DataFrame] = {
    val reader = new ReaderFactory[GlobalConfig](sparkSession, globalConfig, sourceConfig, Map(SchemaManagerParser -> schemaManager, SchemaManagerReader -> schemaManagerReader))
      .getInstance(this.readerType)
    reader.read()
  }

  override protected def transform(dataframe: Option[DataFrame]): DataFrame = {
    val transformation = new TransformationFactory[GlobalConfig](sparkSession, globalConfig,schemaManager)
      .getInstance(this.transformationType)
    transformation.transform(dataframe)
  }

  override protected def write(dataframe: DataFrame): Unit = {
    val writer = new WriterFactory[GlobalConfig](sparkSession, globalConfig, schemaManager).getInstance(this.writerType)
    writer.write(dataframe)
  }

  override protected def finalizeJob(): Unit = sparkSession.stop()
}
