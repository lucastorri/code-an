package com.thoughtworks.dod.out

import com.thoughtworks.dod._
import com.mongodb.casbah.Imports._
import com.mongodb.casbah.commons.{MongoDBList, MongoDBObject}


trait OutputFormatter {
    def export(analyzer: Analyzer, r: Result)

    def close
}

object sysout extends OutputFormatter {

    val outputSeparator = "="
    val columnSeparator = " | "
    val labelDataSeparator = "-"

    def export(analyzer: Analyzer, r: Result) = {
        println(analyzer.desc)
        val sizes = r.labels.map(_.size).toArray
        r.rows.foreach { row =>
            row.zipWithIndex.foreach { case (column, i) => sizes(i) = math.max(sizes(i), column.toString.size) }
        }
        val sepSize = (sizes.sum + ((sizes.size - 1) * columnSeparator.size))
        println(outputSeparator * sepSize)
        println(r.labels.zipWithIndex.map { case (label, i) => label.padTo(sizes(i), ' ') }.mkString(columnSeparator))
        println(labelDataSeparator * sepSize)
        r.rows.foreach { row =>
            println(row.zipWithIndex.map { case (column, i) => column.toString.padTo(sizes(i), ' ') }.mkString(columnSeparator))
        }
        println(outputSeparator * sepSize)
        println()
    }

    def close = {}
}

case class MongoDBFormatter(params: Map[String, String]) extends OutputFormatter {

    def this() = this(Map())

    val dbconnection = MongoConnection(
        params.get("host").getOrElse("localhost"),
        params.get("port").map(_.toInt).getOrElse(27017))

    def export(analyzer: Analyzer, r: Result) = {
        val doc = analyzer.getClass.getName

        val collection = dbconnection("code-an")(doc)
        collection.drop
        val data = r.rows.map(row => MongoDBObject(r.labels.zip(row):_*))
        collection.insert(MongoDBObject("data" -> data, "desc" -> analyzer.desc))
    }

    def close =
        dbconnection.close

}