package com.org.batch.utils

import scala.io.Source
import java.io.InputStream

class Utils {
  def getResourceAsString(path: String): String = {
    try {
      val stream: InputStream = getClass.getResourceAsStream(s"/${path}")
      Source.fromInputStream(stream).getLines().mkString("")
    } catch {
      case ex: Exception => throw ex
    }
  }
}
