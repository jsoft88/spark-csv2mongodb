package com.org.batch.schemas

import io.circe.Json
import org.apache.spark.sql.types.{ArrayType, IntegerType, StringType, StructField, StructType}
import org.scalatest.funsuite.AnyFunSuite

class SchemaManagerTest extends AnyFunSuite {
  test("when correct json is provided, schema is parsed") {
    val schemaManager = new SchemaManager("movies_metadata", SchemaManagerParser)
    assert(!schemaManager.getSchemaAsJson().equals(Json.Null))
  }

  test("A parsed schema is retrievable in struct type format for dataframes schema usability") {
    val schemaManager = new SchemaManager("movies_metadata", SchemaManagerParser)
    val st = schemaManager.getSchemaAsStructType()
    assert(st.fields.filter(f => f.name.equals("genres")).head.dataType.typeName.equals("array"))
    assert(st.contains(StructField("genres", ArrayType(StructType(Seq(StructField("id", IntegerType, false), StructField("name", StringType, false)))))))
  }
}
