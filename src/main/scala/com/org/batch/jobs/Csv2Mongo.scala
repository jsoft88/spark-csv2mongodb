package com.org.batch.jobs

import com.org.batch.config.files.Parser
import com.org.batch.config.{GlobalConfig, JobConfig}
import com.org.batch.core.SparkJob
import com.org.batch.factory.{ReaderFactory, TransformationFactory, WriterFactory}
import org.apache.spark.sql.{DataFrame, SparkSession}

class Csv2Mongo[+T <: JobConfig](config: T) extends SparkJob(config) {
  private var sparkSession: SparkSession = _
  private var readerType: String = ""
  private var transformationType: String = ""
  private var writerType: String = ""
  private val globalConfig = config.asInstanceOf[GlobalConfig]
  private var sourceConfig: Parser = _

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
    globalConfig.csvDelimiter match {
      case None => this.sourceConfig = new Parser("")
      case Some(sc) => this.sourceConfig = new Parser(sc)
    }
  }

  override protected def read(): Option[DataFrame] = {
    val reader = new ReaderFactory[GlobalConfig](sparkSession, globalConfig, sourceConfig).getInstance(this.readerType)
    reader.read()
  }

  override protected def transform(dataframe: Option[DataFrame]): DataFrame = {
    val transformation = new TransformationFactory[GlobalConfig](sparkSession, globalConfig).getInstance(this.transformationType)
    transformation.transform(dataframe)
  }

  override protected def write(dataframe: DataFrame): Unit = {
    val writer = new WriterFactory[GlobalConfig](sparkSession, globalConfig).getInstance(this.writerType)
    writer.write(dataframe)
  }

  override protected def finalizeJob(): Unit = sparkSession.stop()
}
