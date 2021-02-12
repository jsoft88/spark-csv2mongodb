package com.org.batch.readers

import com.org.batch.config.files.Parser
import com.org.batch.config.{GlobalConfig, JobConfig}
import com.org.batch.schemas.SchemaManager
import org.apache.spark.sql.{DataFrame, SparkSession, functions}

class CsvReader[+T <: JobConfig](sparkSession: SparkSession, config: T, configFile: Parser, schemaManager: SchemaManager)
  extends {
    private var inputSource = ""
    private var readerOptions: Map[String, String] = Map.empty
} with BaseReader[T](sparkSession, config, configFile, schemaManager) {

  override protected def setupReader(): Unit = {
    this.config.asInstanceOf[GlobalConfig].inputSource match {
      case None => throw new Exception("Expected input source to be present, but None found")
      case Some(is) => this.inputSource = is
    }
    this.readerOptions = this.configFile.getSparkOptions()
  }

  override def read(): Option[DataFrame] = {
    val df = this.sparkSession.read.options(this.readerOptions).csv(this.inputSource)
    this.configFile.getJsonFields() headOption match {
      case None => Some(df)
      case Some(_) => {
        var finalDF = df.select(df.columns.filterNot(c => this.configFile.getJsonFields.contains(c)).map(functions.col(_)): _*)
        this.configFile.getJsonFields().foreach(f => {
          finalDF = finalDF.withColumn(f, functions.from_json(df(f), this.schemaManager.getSchemaForField(f)))
        })
        Some(finalDF)
      }
    }
  }
}
