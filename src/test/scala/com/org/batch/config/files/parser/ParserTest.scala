package com.org.batch.config.files.parser

import com.org.batch.config.files.Parser
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

class ParserTest extends AnyFunSuite with BeforeAndAfterAll {
  var parser: Parser = _

  override protected def beforeAll(): Unit = {
    this.parser = new Parser("movies_metadata")
  }

  test("All json fields are retrieved from config file") {
    this.parser.getJsonFields().foreach(f => {
      assert(Seq(
        "belongs_to_collection",
        "genres",
        "production_companies",
        "production_countries",
        "spoken_languages"
      ).contains(f))
    })
  }

  test("schema is retrieved for a given field") {
    this.parser.getSchemaPerJsonField("genres").fields.foreach(f => {
      assert(Seq("id", "name").contains(f.name))
    })
  }

  test("Exception is thrown when schema is requested for non-existing field") {
    assertThrows[Exception]{
      this.parser.getSchemaPerJsonField("dummy")
    }
  }

  test("Exception is thrown when the config key passed does not exist") {
    assertThrows[Exception] {
      new Parser("dummy")
    }
  }
}
