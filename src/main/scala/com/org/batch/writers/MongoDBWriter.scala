package com.org.batch.writers

import com.mongodb.spark.MongoSpark
import com.org.batch.config.{GlobalConfig, JobConfig}
import com.org.batch.schemas.SchemaManager
import org.apache.spark.sql.{DataFrame, SparkSession}

class MongoDBWriter[+T <: JobConfig](sparkSession: SparkSession, config: T, schemaManager: SchemaManager)
  extends BaseWriter[T](sparkSession, config, schemaManager) {
  override def setupWriter(): Unit = {}

  override def write(dataframe: DataFrame): Unit = {
    MongoSpark.save(dataframe)
  }
}
