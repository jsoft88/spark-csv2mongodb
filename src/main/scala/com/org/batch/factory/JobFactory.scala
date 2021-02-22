package com.org.batch.factory

import com.org.batch.config.{GlobalConfig}
import com.org.batch.core.SparkJob
import com.org.batch.jobs.Csv2Mongo

trait JobType

object JobFactory {
  case object Csv2Mongo extends JobType {
    override def toString: String = "csv2mongo"
  }

  val AllTypes = Seq(
    Csv2Mongo
  )
}
class JobFactory(config: GlobalConfig) {
  def getInstance(jobType: JobType): SparkJob = {
    jobType match {
      case JobFactory.Csv2Mongo => new Csv2Mongo(config)
      case _ => throw new Exception("Unable to find suitable job for provided type")
    }
  }

  def getInstance(jobType: String): SparkJob = {
    JobFactory.AllTypes.filter(_.toString.equals(jobType)).headOption match {
      case None => throw new Exception(s"Invalid job type provided: ${jobType}")
      case Some(jt) => this.getInstance(jt)
    }
  }
}
