package com.org.batch.transformations

import com.org.batch.config.JobConfig
import com.org.batch.schemas.SchemaManager
import org.apache.spark.sql.{DataFrame, SparkSession}

class NoOpTransformation [+T <: JobConfig](sparkSession: SparkSession, config: T, schemaManager: SchemaManager)
  extends BaseTransformation[T](sparkSession, config, schemaManager) {
  override def setupTransformation(): Unit = {}

  override def transform(dataframe: Option[DataFrame]): DataFrame = {
    dataframe match {
      case None => throw new Exception("Dataframe to be transformed was None")
      case Some(df) => df
    }
  }
}
