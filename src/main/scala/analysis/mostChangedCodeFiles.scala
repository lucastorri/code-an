package co.torri.dod.analysis

import spark._
import com.thoughtworks.dod._


class MostChangedCodeFileAnalyzer extends Analyzer {
    val desc = "Code File With Biggest Number of Checkins"

    def apply(data: RepoData, sc: SparkContext) = {
        val codeExtensions = List("java", "rb", "js", "htm", "html", "jsp", "css")

        Result(Seq("file", "checkins"),
            data.commits
                .flatMap(_.files)
                .filter(_.isOfType(codeExtensions))
                .map(_.path)
                .countByValue
                .toList
                .sortBy { case (file, count) => - count }
                .map(_.productIterator.toList))
    }

}