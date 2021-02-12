package com.org.batch.schemas

import com.org.batch.types.Management
import com.org.batch.utils.Utils
import io.circe.Json
import io.circe._
import io.circe.parser._
import org.apache.spark.sql.types.{ArrayType, DataType, StructField, StructType}


class SchemaManager(source: String) {
  var schema: Option[Json] = None
  var schemaAsStruct: Option[StructType] = None

  def getSchemaAsJson(): Json = {
    this.schema match {
      case None => throw new Exception("Json apparently was not parsed or an error occurred while parsing")
      case Some(js) => js
    }
  }

  def parseSchema(): Unit = {
    val contentAsString = new Utils().getResourceAsString(s"schemas/parser/${this.source}.json")
    parse(contentAsString) match {
      case Left(err) => throw new Exception(s"An error occurred while parsing json schema of source ${source}. Details: ${err.getMessage()}")
      case Right(js) => this.schema = Some(js); this.getSchemaAsStructType()
    }
  }

  private[schemas] def getField(t: String, json: Json*): StructField = {
    t match {
      case "array" => {
        StructField(
          json.head.hcursor.downField("name").as[String] match {
            case Left(err) => throw err
            case Right(n) => n
          },
          ArrayType(
            json.head.hcursor.downField("type").downField("items").as[String] match {
              case Left(_) => {
                StructType(
                  json.head.hcursor.downField("type").downField("items").downField("fields").focus.flatMap(_.asArray)
                    .getOrElse(Vector.empty).map(jsf => this.getField(jsf.hcursor.downField("type").as[String] match {
                    case Left(err) => throw err
                    case Right(at) =>at
                  }, jsf))
                )
              }
              case Right(st) => this.getField(st, json.head).dataType
            }
          )
        )
      }
      case "record" => {
        // parse the inner record
        StructField(json.head.hcursor.downField("name").as[String] match {
          case Left(err) => throw err
          case Right(n) => n
        },
          StructType(json.head.hcursor.downField("fields").focus.flatMap(_.asArray).getOrElse(Vector.empty)
            .map(js => this.getField(js.hcursor.downField("type").as[String] match {
              case Left(err) => throw err
              case Right(it) => it
            }, js))))
      }
      case st => StructField(
        json.head.hcursor.downField("name").as[String].right.get,
        Management.getStructType(st),
        json.head.hcursor.downField("nullable").as[Boolean].right.get)
    }
  }

  def getSchemaAsStructType(): StructType = {
    this.schema match {
      case None => throw new Exception("Apparently schema was not yet parsed from json file. Please invoke parseSchema before invoking this method")
      case Some(s) => {
        this.schemaAsStruct match {
          case None => {
            this.schemaAsStruct = Some(
              StructType(
                s.hcursor.downField("fields").focus.flatMap(_.asArray).getOrElse(Vector.empty)
                  .map(js => {
                    js.hcursor.downField("type").as[String] match {
                      /* getting as string fails when type is a json object
                      Example: { "name": "f", "type": { "type": "array", "items": "string" } }
                      */
                      case Left(_) => this.getField("array", js)
                      /* getting as string works when type is not an array
                      Example: { "name": "f", "type": "string" } or { "name": "f", "type": "record", "fields": [...] }
                      */
                      case Right(t) => this.getField(t, js)
                    }
                  })
              )
            )
            this.schemaAsStruct.get
          }
          case Some(_) => this.schemaAsStruct.get
        }
      }
    }
  }

  def getSchemaForField(f: String): StructType = {
    this.schema match {
      case None => throw new Exception("No schema has been parsed for reader")
      case Some(_) => this.getSchemaAsStructType().filter(sf => sf.name.equals(f)).headOption match {
        case None => throw new Exception(s"Provided field `${f}` to extract from struct not found")
        case Some(fsf) => StructType(Seq(fsf))
      }
    }
  }
}