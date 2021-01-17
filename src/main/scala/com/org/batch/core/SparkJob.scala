package com.org.batch.core

import com.org.batch.config.JobConfig
import org.apache.logging.log4j.scala.Logging
import org.apache.spark.sql.{DataFrame}

abstract class SparkJob(config: JobConfig) extends Logging {

  protected def setupJob(): Unit

  protected def read(): Option[DataFrame]

  protected def transform(dataframe: Option[DataFrame]): DataFrame

  protected def write(dataframe: DataFrame): Unit

  protected def finalizeJob(): Unit

  final def run(): Unit = {
    try {
      this.logger.info("Setting up job...")
      this.setupJob()
      this.logger.info("Getting ready for creating dataframe from input source...")
      val df = this.read()
      this.logger.info("Dataframe loaded, now preparing for transformation...")
      val transformedDF = this.transform(df)
      this.logger.info("Transformation completed, proceeding to writing of dataframe...")
      this.write(transformedDF)
    } catch {
      case ex => this.logger.fatal(s"error is -> ${ex.getMessage}. Stacktrace --> ${ex.getStackTrace.map(_.toString)}")
    } finally {
      this.logger.info("Finalizing job")
      this.finalizeJob()
    }

  }
}
