package com.org.batch.writers

import com.org.batch.config.JobConfig
import org.apache.spark.sql.{DataFrame, SparkSession}

abstract class BaseWriter[+T <: JobConfig](sparkSession: SparkSession, config: T) {
  def setupWriter(): Unit

  def write(dataframe: DataFrame): Unit
}
