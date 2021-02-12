package com.org.batch.readers

import com.org.batch.config.JobConfig
import com.org.batch.config.files.Parser
import com.org.batch.schemas.SchemaManager
import org.apache.spark.sql.{DataFrame, SparkSession}

abstract class BaseReader[+T <: JobConfig](sparkSession: SparkSession, config: T, configFile: Parser, schemaManager: SchemaManager) {
  this.setupReader()

  protected def setupReader(): Unit

  def read(): Option[DataFrame]
}
