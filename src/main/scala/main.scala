package com.thoughtworks.dod

import spark._
import SparkContext._
import scala.collection.JavaConversions._
import org.reflections.Reflections
import com.thoughtworks.dod.parsers._


object main {

    def main(args: Array[String]) {

        val (gitlog, jiralog) = ("git.log", "jira.log")

        val sc = new SparkContext("local[4]", "code-an")
        val data = RepoData(
            sc.toRDD(gitlog),
            sc.toRDD(jiralog))

        val analyzersClasses = args.headOption
            .map { className =>
                List(Class.forName(className).asInstanceOf[Class[Analyzer]])
            }
            .getOrElse {
                new Reflections("")
                    .getSubTypesOf(classOf[Analyzer])
                    .filterNot(_.isAnnotationPresent(classOf[Deactivated]))
                    .toList
            }

        analyzersClasses.map(_.newInstance).foreach { an =>
            out.println(an.desc, an(data, sc))
        }
    }

}