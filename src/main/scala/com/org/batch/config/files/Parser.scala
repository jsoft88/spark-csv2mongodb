package com.org.batch.config.files

import com.org.batch.utils.Utils
import io.circe
import io.circe._
import io.circe.parser._

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
    val optionsCursor = this.configAsJson.hcursor.downField("sparkOptions").focus.get.hcursor
    optionsCursor.keys match {
      case None => Map.empty
      case Some(keys) => keys.map(k => {
        val value = optionsCursor.get[String](k) match {
          case Left(_) => throw new Exception("Error while parsing json object sparkOptions in config file")
          case Right(value) => value
        }
        k -> value
      }).toMap
    }
  }
}
