package com.org.batch.factory

import com.org.batch.config.JobConfig
import com.org.batch.transformations.{BaseTransformation, NoOpTransformation}
import org.apache.spark.sql.SparkSession

sealed trait TransformationType

object TransformationFactory {
  case object NoOp extends TransformationType {
    override def toString: String = "no-op"
  }

  val AllTransformations = Seq(
    TransformationFactory.NoOp
  )
}

class TransformationFactory[+T <: JobConfig](sparkSession: SparkSession, config: T) {
  def getInstance(transformationType: TransformationType): BaseTransformation[T] = {
    transformationType match {
      case TransformationFactory.NoOp => new NoOpTransformation[T](sparkSession, config)
      case _ => throw new Exception("Could not find a suitable transformation for the provided type")
    }
  }

  def getInstance(transformationType: String): BaseTransformation[T] = {
    TransformationFactory.AllTransformations.filter(_.toString.equals(transformationType)).headOption match {
      case None => throw new Exception(s"Invalid transformation type string: ${transformationType}")
      case Some(t) => this.getInstance(t)
    }
  }
}
