package com.thoughtworks.dod

import spark._
import SparkContext._
import scala.collection.JavaConversions._
import org.reflections.Reflections
import com.thoughtworks.dod.parsers._


object main {

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
            out.println(an.desc, an(data, sc))
        }
    }

}