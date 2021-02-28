package com.org.batch.writers

import com.org.batch.config.{GlobalConfig, JobConfig}
import com.org.batch.schemas.SchemaManager
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}


class MySQLWriter[+T <: JobConfig](sparkSession: SparkSession, config: T, schemaManager: SchemaManager) extends {
  var mysqlTable = ""
  var mysqlUri = ""
  var mysqlDB = ""
  var mysqlUsername = ""
  var mysqlPassword = ""
  var jdbcDriver = ""
} with BaseWriter[T](sparkSession, config, schemaManager) {
  override def setupWriter(): Unit = {
    config.asInstanceOf[GlobalConfig].mongoOutputUri match {
      case None => throw new Exception("Missing URI parameter for MySQL")
      case Some(uri) => this.mysqlUri = uri
    }

    config.asInstanceOf[GlobalConfig].mongoOutputCollection match {
      case None => throw new Exception("Missing output table in MySQL")
      case Some(tb) => this.mysqlTable = tb
    }

    config.asInstanceOf[GlobalConfig].mongoOutputDatabase match {
      case None => throw new Exception("Missing output database in MySQL")
      case Some(db) => this.mysqlDB = db
    }

    config.asInstanceOf[GlobalConfig].mongoPasswordEnvKey match {
      case None => throw new Exception("Missing environment variable name containing MySQL password")
      case Some(envKeyPassword) => sys.env.get(envKeyPassword) match {
        case None => throw new Exception(s"Environment variable ${envKeyPassword} is empty")
        case Some(envValue) => this.mysqlPassword = envValue
      }
    }

    config.asInstanceOf[GlobalConfig].mongoUsernameEnvKey match {
      case None => throw new Exception("Missing environment variable name containing MySQL username")
      case Some(envKeyUsername) => sys.env.get(envKeyUsername) match {
        case None => throw new Exception(s"Environment variable ${envKeyUsername} is empty")
        case Some(envValue) => this.mysqlUsername = envValue
      }
    }

    config.asInstanceOf[GlobalConfig].dbDriver match {
      case None => throw new Exception("Missing jdbc driver class")
      case Some(jdrvr) => this.jdbcDriver = jdrvr
    }
  }

  override def write(dataframe: DataFrame): Unit = {
    dataframe.write
      .mode(SaveMode.Overwrite)
      .option("url", this.mysqlUri)
      .option("dbTable", s"${this.mysqlDB}.${this.mysqlTable}")
      .option("user", this.mysqlUsername)
      .option("password", this.mysqlPassword)
      .option("driver", this.jdbcDriver)
      .format("jdbc")
      .save()
  }
}
