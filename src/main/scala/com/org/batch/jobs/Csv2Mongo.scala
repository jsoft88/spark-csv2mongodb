package com.org.batch.jobs

import com.org.batch.config.files.Parser
import com.org.batch.config.{GlobalConfig, JobConfig}
import com.org.batch.core.SparkJob
import com.org.batch.factory.{ReaderFactory, TransformationFactory, WriterFactory}
import com.org.batch.schemas.SchemaManager
import org.apache.spark.sql.{DataFrame, SparkSession}

class Csv2Mongo[+T <: JobConfig](config: T) extends SparkJob(config) {
  private var sparkSession: SparkSession = _
  private var readerType: String = ""
  private var transformationType: String = ""
  private var writerType: String = ""
  private val globalConfig = config.asInstanceOf[GlobalConfig]
  private var sourceConfig: Parser = _
  private var schemaManager: SchemaManager = _

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

    var sparkSessionBuilder = SparkSession.builder()
    globalConfig.mongoOutputUri match {
      case None => throw new Exception("Missing mongo output uri in configuration")
      case Some(mou) => sparkSessionBuilder = sparkSessionBuilder.config("spark.mongodb.output.uri", mou)
    }

    globalConfig.mongoInputUri match {
      case None => throw new Exception("Missing mongo input uri in configuration")
      case Some(miu) => sparkSessionBuilder.config("spark.mongodb.input.uri", miu)
    }

    this.sparkSession = sparkSessionBuilder.appName("csv2mongo").getOrCreate()
    globalConfig.readerConfigKey match {
      case None => throw new Exception("Missing configuration key for config file under resources/reader_config")
      case Some(sc) => this.sourceConfig = new Parser(sc); this.schemaManager = new SchemaManager(sc)
    }
  }

  override protected def read(): Option[DataFrame] = {
    val reader = new ReaderFactory[GlobalConfig](sparkSession, globalConfig, sourceConfig, schemaManager)
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
