package com.thoughtworks.dod

import spark._
import SparkContext._
import scala.collection.JavaConversions._
import org.reflections.Reflections
import com.thoughtworks.dod.parsers._


object main {

    val outputSeparator = "="
    val columnSeparator = " | "
    val labelDataSeparator = "-"

    def main(args: Array[String]) {

        val (gitlog, jiralog) = args match {
            case Array(g, j) => (g,j)
            case _ => ("git.log", "jira.log")
        }

        val sc = new SparkContext("local[2]", "code-an")
        val data = RepoData(
            sc.toRDD[Commit](gitlog),
            sc.toRDD[Issue](jiralog))

        val reflections = new Reflections("")
        val analyzersClasses = reflections
            .getSubTypesOf(classOf[Analyzer])
            .filterNot(_.isAnnotationPresent(classOf[Deactivated]))

        analyzersClasses.foreach { ac =>
            val an = ac.newInstance.asInstanceOf[Analyzer]
            val r = an(data, sc)
            println(an.desc)
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
    }

}