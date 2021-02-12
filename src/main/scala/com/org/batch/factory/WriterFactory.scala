package com.org.batch.factory

import com.org.batch.config.JobConfig
import com.org.batch.schemas.SchemaManager
import com.org.batch.writers.{BaseWriter, MongoDBWriter}
import org.apache.spark.sql.SparkSession

sealed trait WriterType

object WriterFactory {
  case object MongoDBWriter extends WriterType {
    override def toString: String = "mongodb"
  }

  val AllWriters = Seq(
    WriterFactory.MongoDBWriter
  )
}

class WriterFactory[+T <: JobConfig](sparkSession: SparkSession, config: T, schemaManager: SchemaManager) {
  def getInstance(writerType: WriterType): BaseWriter[T] = {
    writerType match {
      case WriterFactory.MongoDBWriter => new MongoDBWriter[T](sparkSession, config, schemaManager)
      case _ => throw new Exception("Could not find suitable writer instance for provided type")
    }
  }

  def getInstance(writerType: String): BaseWriter[T] = {
    WriterFactory.AllWriters.filter(_.toString.equals(writerType)).headOption match {
      case None => throw new Exception(s"Invalid writer type string: ${writerType}")
      case Some(t) => this.getInstance(t)
    }
  }
}
