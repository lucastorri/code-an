package com.thoughtworks.dod

import spark._
import scala.io._
import scala.annotation._
import java.util.{Date, Calendar}


case class Commit(
    hash: String,
    author: String,
    time: Date,
    project: Option[String],
    story: Option[Int],
    message: String,
    files: List[FileChange]) {

    lazy val yearMonth = {
        val cal = Calendar.getInstance
        cal.setTime(time)
        (cal.get(Calendar.YEAR), cal.get(Calendar.MONTH))
    }
}

object Commit {
    def apply(hash: String,
        author: String,
        time: Long,
        project: Option[String],
        story: Option[Int],
        message: String,
        files: List[FileChange]) : Commit = apply(hash, author, new Date(time*1000), project, story, message, files)
}

case class FileChange(
    path: String,
    added: Int,
    removed: Int) {

    lazy val workspace =
        Option(path)
            .filter(_.contains("/"))
            .map(p => p.substring(0, p.indexOf("/", 1)).replaceAll("/", "").replaceAll("\"", ""))

    def isOfType(sufix: String) : Boolean =
        path.endsWith("." + sufix)

    def isOfType(sufixes: List[String]) : Boolean =
        sufixes.exists(s => isOfType(s))
}

case class Issue(
    title: String,
    project: String,
    story: Int,
    typeOf: String,
    status: String,
    devs: Option[Seq[String]]) {

    lazy val id = "%s-%d".format(project, story)
}

case class RepoData(
    commits: RDD[Commit],
    issues: RDD[Issue])

trait Analyzer {

    def desc: String

    def apply(data: RepoData, sc: SparkContext) : Result
}

case class Result(labels: Seq[String] = Nil, rows: Seq[Seq[_]] = Nil)
