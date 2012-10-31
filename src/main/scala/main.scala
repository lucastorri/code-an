package com.thoughtworks.dod

import spark._
import SparkContext._
import scala.collection.JavaConversions._
import org.reflections.Reflections
import com.thoughtworks.dod.out._
import com.thoughtworks.dod.parsers._
import com.mosesn.pirate.Pirate


object main {

    val inputFormat = "[-a string] [-f string] [-c string] [input_folder]"
    val (gitLogExtension, jiraExportExtension) = (".commit", ".jira")

    def main(args: Array[String]) {
        val params = Pirate(inputFormat)(args).strings

        val analyzersClasses = params.get("a")
            .map (a => Iterable(Class.forName(a).asInstanceOf[Class[Analyzer]]))
            .getOrElse(allAnalyzers)
        val outputFormatter = params.get("f")
            .map(f => Class.forName(f).newInstance.asInstanceOf[OutputFormatter])
            .getOrElse(sysout)
        val connectionString = params.get("c")
            .getOrElse("local[4]")
        val inputFolder = params.get("input_folder")
            .getOrElse(".")


        val sc = new SparkContext(connectionString, "code-an")
        val data = RepoData(
            sc.toRDD(inputFolder.filesEndingWith(gitLogExtension)),
            sc.toRDD(inputFolder.filesEndingWith(jiraExportExtension)))

        analyzersClasses.map(_.newInstance)
            .foreach(an => outputFormatter.export(an, an(data, sc)))
    }

    def allAnalyzers =
        new Reflections("")
            .getSubTypesOf(classOf[Analyzer])
            .filterNot(_.isAnnotationPresent(classOf[Deactivated]))
}