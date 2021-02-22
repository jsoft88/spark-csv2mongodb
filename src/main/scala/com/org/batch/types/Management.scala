package com.org.batch.types

import org.apache.spark.sql.types.{BooleanType, DataType, DoubleType, FloatType, IntegerType, StringType, StructField}

object Management {
  def getStructType(jsonType: String): DataType = {
    jsonType.toLowerCase match {
      case "string" => StringType
      case "int" => IntegerType
      case "boolean" => BooleanType
      case "float" => FloatType
      case "double" => DoubleType
      case _ => throw new Exception(s"Invalid type for StructType provided: ${jsonType}")
    }
  }

  def getStructField(name: String, jsonType: String): StructField = {
    StructField(name, this.getStructType(jsonType))
  }
}
