package com.thoughtworks.dod

import spark._
import SparkContext._
import scala.collection.JavaConversions._
import org.reflections.Reflections
import com.thoughtworks.dod.parsers._
import java.io.File


object main {

    def main(args: Array[String]) {

        val (gitLogExtension, jiraExportExtension) = (".commit", ".jira")

        val sc = new SparkContext("local[4]", "code-an")
        val data = RepoData(
            sc.toRDD(filesEndingWith(gitLogExtension)),
            sc.toRDD(filesEndingWith(jiraExportExtension)))

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
            out.export(an.desc, an(data, sc))
        }
    }

    def filesEndingWith(sufix: String) =
        new File(".").listFiles.filter(_.getName.endsWith(sufix)).map(_.getCanonicalPath)

}