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
    val FormatterString = """([a-zA-Z][a-zA-Z\d\.]*)\[(.*)?\]""".r
    val ParamFormat = "([^=]+)(=([^=]+)){0,1}".r
    val (gitLogExtension, jiraExportExtension) = (".commit", ".jira")

    def main(args: Array[String]) {
        val params = Pirate(inputFormat)(args).strings

        val analyzersClasses = params.get("a")
            .map (a => Iterable(Class.forName(a).asInstanceOf[Class[Analyzer]]))
            .getOrElse(allAnalyzers)
        val outputFormatter = params.get("f")
            .map(formatter)
            .getOrElse(sysout)
        val sparkConnection = params.get("c")
            .getOrElse("local[4]")
        val inputFolder = params.get("input_folder")
            .getOrElse(".")


        val sc = new SparkContext(sparkConnection, "code-an")
        val data = RepoData(
            sc.toRDD(inputFolder.filesEndingWith(gitLogExtension)),
            sc.toRDD(inputFolder.filesEndingWith(jiraExportExtension)))

        analyzersClasses.map(_.newInstance)
            .foreach(an => outputFormatter.export(an, an(data, sc)))

        outputFormatter.close
    }

    def formatter: PartialFunction[String, OutputFormatter] = {
        case FormatterString(clazz, params) =>
            val paramsMap = params.split(",")
                .filter(_.nonEmpty)
                .map { case ParamFormat(key, _, value) => (key, value) }
                .toMap
            Class.forName(clazz)
                .getConstructor(classOf[Map[_,_]])
                .newInstance(paramsMap)
                .asInstanceOf[OutputFormatter]
        case clazz =>
            Class.forName(clazz)
                .newInstance
                .asInstanceOf[OutputFormatter]

    }

    def allAnalyzers =
        new Reflections("")
            .getSubTypesOf(classOf[Analyzer])
            .filterNot(_.isAnnotationPresent(classOf[Deactivated]))
}