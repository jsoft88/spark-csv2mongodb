package com.org.batch.writers

import com.mongodb.spark.MongoSpark
import com.mongodb.spark.config._
import com.org.batch.config.{GlobalConfig, JobConfig}
import com.org.batch.schemas.SchemaManager
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

class MongoDBWriter[+T <: JobConfig](sparkSession: SparkSession, config: T, schemaManager: SchemaManager)
  extends {
    private var writeConfig: WriteConfig = null
  } with BaseWriter[T](sparkSession, config, schemaManager) {
  override def setupWriter(): Unit = {
    var mapWriteConfig: Map[String, String] = Map.empty
    config.asInstanceOf[GlobalConfig].mongoOutputUri match {
      case None => throw new Exception("Missing mongo output uri in configuration")
      case Some(mou) => mapWriteConfig = mapWriteConfig ++ Map("spark.mongodb.output.uri" -> mou)
    }
    config.asInstanceOf[GlobalConfig].mongoOutputDatabase match {
      case None => throw new Exception("Missing mongo output database")
      case Some(od) => mapWriteConfig = mapWriteConfig ++ Map("spark.mongodb.output.database" -> od)
    }

    config.asInstanceOf[GlobalConfig].mongoOutputCollection match {
      case None => throw new Exception("Missing mongo output collection")
      case Some(oc) => mapWriteConfig = mapWriteConfig ++ Map("spark.mongodb.output.collection" -> oc)
    }

    this.writeConfig = WriteConfig(mapWriteConfig)
  }

  override def write(dataframe: DataFrame): Unit = {
    MongoSpark.save(dataframe.write.mode(SaveMode.Overwrite), writeConfig)
  }
}
