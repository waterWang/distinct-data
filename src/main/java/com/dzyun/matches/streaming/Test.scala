package com.dzyun.matches.streaming


import scala.util.Success

object Test {

  def main(args: Array[String]): Unit = {

    System.out.println(str2Long("NULL"))
  }

  def str2Long(s: String): Long = {
    val r1 = scala.util.Try(s.toLong)
    r1 match {
      case Success(_) => s.toLong;
      case _ => -1L
    }
  }

}
