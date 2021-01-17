package com.org.batch.config.files

import com.org.batch.types.Management
import com.org.batch.utils.Utils
import io.circe
import io.circe._
import io.circe.parser._
import org.apache.spark.sql.types.{StructField, StructType}

class Parser(forSource: String) {
  val configAsJson = this.parse()

  private def parse(): Json = {
    if (forSource.isEmpty) {
      return parser.parse("{}") match {
        case Right(json) => json
      }
    }
    val configContent = new Utils().getResourceAsString(s"reader_config/${forSource}.json")
    circe.parser.parse(configContent) match {
      case Left(err) => throw new Exception(s"${err.message} ---> ${err.getStackTrace.mkString("\n")}")
      case Right(json) => json
    }
  }

  def getJsonFields(): Seq[String] = {
    this.configAsJson.hcursor.get[Seq[String]]("jsonFields") match {
      case Left(err) => throw new Exception(s"Malformed config json as no field 'jsonFields' was found --> ${err.message}")
      case Right(jsonFields) => jsonFields
    }
  }

  def getSparkOptions(): Map[String, String] = {
    this.configAsJson.hcursor.get[Map[String, String]]("sparkOptions") match {
      case Left(err) => throw new Exception(s"Malformed config json as no field 'sparkOptions' was found --> ${err.message}")
      case Right(sparkOptions) => sparkOptions
    }
  }

  def getSchemaPerJsonField(field: String): StructType = {
    this.configAsJson.hcursor.downField("schemaPerJsonField").focus.flatMap(_.asArray).getOrElse(Vector.empty)
      .filter(jo => jo.hcursor.downField(field).focus.flatMap(_.asArray).headOption.isDefined)
      .headOption match {
      case None => throw new Exception(s"Field name not found in parsed schema: ${field}")
      case Some(jsField) => jsField.hcursor.focus.flatMap(_.asArray).getOrElse(Vector.empty)
        .foldRight(StructType(Seq.empty))((cv, st) => {
          val fieldName = cv.hcursor.get[String]("name") match {
            case Left(err) => throw new Exception(err.message)
            case Right(n) => n
          }

          val fieldType = cv.hcursor.get[String]("type") match {
            case Left(err) => throw new Exception(err.message)
            case Right(t) => t
          }
          st.add(Management.getStructField(fieldName, fieldType))
        })
    }
  }
}
