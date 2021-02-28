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
    config.asInstanceOf[GlobalConfig].mongoOutputDatabase match {
      case None => throw new Exception("Missing mongo output database")
      case Some(od) => mapWriteConfig = mapWriteConfig ++ Map("spark.mongodb.output.database" -> od)
    }

    config.asInstanceOf[GlobalConfig].mongoOutputCollection match {
      case None => throw new Exception("Missing mongo output collection")
      case Some(oc) => mapWriteConfig = mapWriteConfig ++ Map("spark.mongodb.output.collection" -> oc)
    }
    val usernameKey = config.asInstanceOf[GlobalConfig].mongoUsernameEnvKey match {
      case None => throw new Exception("Missing mongo username key to search in environment variables")
      case Some(uk) => uk
    }

    val passwordKey = config.asInstanceOf[GlobalConfig].mongoPasswordEnvKey match {
      case None => throw new Exception("Missing mongo password key to search in environment variables")
      case Some(pk) => pk
    }

    config.asInstanceOf[GlobalConfig].mongoOutputUri match {
      case None => throw new Exception("Missing mongo output uri in configuration")
      case Some(mou) => {
        val username = sys.env.get(usernameKey) match {
          case None => throw new Exception(s"Key ${usernameKey} not found in environment variables while searching for username")
          case Some(v) => v
        }
        val password = sys.env.get(passwordKey) match {
          case None => throw new Exception(s"Key ${passwordKey} not found in environment variables while searching for password")
          case Some(v) => v
        }
        val augmentedUri = mou.replace("mongodb://", s"mongodb://${username}:${password}@")
        mapWriteConfig = mapWriteConfig ++ Map("spark.mongodb.output.uri" -> augmentedUri)
      }
    }

    this.writeConfig = WriteConfig(mapWriteConfig)
  }

  override def write(dataframe: DataFrame): Unit = {
    MongoSpark.save(dataframe.write.mode(SaveMode.Overwrite), writeConfig)
  }
}
