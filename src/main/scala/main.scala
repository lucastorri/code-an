package com.thoughtworks.dod

import spark._
import SparkContext._
import scala.io.Source
import scala.collection.JavaConversions._
import org.reflections.Reflections
import scala.xml.XML


object main {

    val outputSeparator = "="
    val columnSeparator = " | "
    val labelDataSeparator = "-"

    def main(args: Array[String]) {

        val (gitlog, jiralog) = args match {
            case Array(g, j) => (g,j)
            case _ => ("git.log", "jira.log")
        }

        val sc = new SparkContext("local[2]", "test")

        val rawCommits = commitParser.breakIndividualCommits(gitlog)
        val commits = sc
            .makeRDD(rawCommits, sc.defaultParallelism)
            .map(commitParser.parseCommit(_))
            .cache

        val issues = sc
            .makeRDD(jiraParser.parse(jiralog), sc.defaultParallelism)
            .cache

        val data = RepoData(commits, issues)

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

object commitParser {
    val binaryFileGitNumstat = "-"
    val Separator = "====="
    val CommitInfo = """([\da-z]+);(.*);(\d+);(.*)""".r
    val MessageFormat = """\s*([^-]+)-(\d+)(.*)""".r
    val FileFormat = """([\d-]+)\s+([\d-]+)\s+(.*)""".r

    def breakIndividualCommits(inputFile: String) = {
        Source.fromFile(inputFile)
            .getLines
            .foldLeft(List[List[String]]()) {
                case (list, "") => list
                case (list, Separator) => Nil :: list
                case (list, e) => (e :: list.head) :: list.tail
            }
    }

    def parseCommit(commit: List[String]) = {
        val commitInfo :: filesInfo = commit.reverse
        val CommitInfo(hash, author, time, fullMessage) = commitInfo
        val files = filesInfo.flatMap { fi =>
            val FileFormat(addedLines, removedLines, path) = fi
            Option(addedLines)
                .filterNot(_.contains(binaryFileGitNumstat))
                .map(_ => FileChange(path, addedLines.toInt, removedLines.toInt))
        }

        val (project, story, message) =
            MessageFormat.unapplySeq(fullMessage)
                .flatMap {
                    case List(p,s,m) => Some(Option(p.toUpperCase), Option(s.toInt), m)
                    case _ => None
                }.getOrElse((None, None, fullMessage))

        Commit(hash, author.toLowerCase, time.toLong, project, story, message, files)
    }

}

object jiraParser {

    val KeyFormat = """([^-]+)-(\d+)""".r

    def parse(inputFile: String) = {
        val items = XML.loadFile(inputFile) \\ "item"
        items.map { i =>
            val title = (i \ "title").text
            val KeyFormat(project, story) = (i \ "key").text
            val typeOf = (i \ "type").text
            val devs = (i \\ "customfield")
                .filter(cf => (cf \ "customfieldname")
                .text.contains("Developer"))
                .headOption
                .map(cf => (cf \\ "customfieldvalue").map(v => v.text))
            Issue(title, project, story.toInt, typeOf, devs)
        }
    }


}