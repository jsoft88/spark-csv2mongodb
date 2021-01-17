package com.org.batch.transformations

import com.org.batch.config.JobConfig
import org.apache.spark.sql.{DataFrame, SparkSession}

abstract class BaseTransformation[+T <: JobConfig](sparkSession: SparkSession, config: T) {
  this.setupTransformation()

  def setupTransformation(): Unit

  def transform(dataframe: Option[DataFrame]): DataFrame
}
