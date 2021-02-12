package com.org.batch.factory

import com.org.batch.config.JobConfig
import com.org.batch.config.files.Parser
import com.org.batch.readers.{BaseReader, CsvReader}
import com.org.batch.schemas.SchemaManager
import org.apache.spark.sql.SparkSession

sealed trait ReaderType

object ReaderFactory {
  case object CsvReader extends ReaderType {
    override def toString: String = "csv"
  }

  val AllReaders = Seq(
    CsvReader
  )
}

class ReaderFactory[+T <: JobConfig](sparkSession: SparkSession, config: T, configFile: Parser, schemaManager: SchemaManager) {
  def getInstance(readerType: ReaderType): BaseReader[T] = {
    readerType match {
      case ReaderFactory.CsvReader => new CsvReader[T](sparkSession, config, configFile, schemaManager)
      case _ => throw new Exception("Could not find suitable reader instance for provided type.")
    }
  }

  def getInstance(readerType: String): BaseReader[T]= {
    ReaderFactory.AllReaders.filter(_.toString.equals(readerType)).headOption match {
      case None => throw new Exception(s"Invalid reader type string given: ${readerType}")
      case Some(s) => this.getInstance(s)
    }
  }
}
