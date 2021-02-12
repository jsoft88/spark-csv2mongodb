package com.org.batch.writers

import com.org.batch.config.JobConfig
import com.org.batch.schemas.SchemaManager
import org.apache.spark.sql.{DataFrame, SparkSession}

abstract class BaseWriter[+T <: JobConfig](sparkSession: SparkSession, config: T, schemaManager: SchemaManager) {
  def setupWriter(): Unit

  def write(dataframe: DataFrame): Unit
}
