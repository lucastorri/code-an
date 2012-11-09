package co.torri.dod.analysis

import spark._
import com.thoughtworks.dod._


class JiraStoryTypesAndStatusAnalyzer extends Analyzer {
    def apply(data: RepoData, sc: SparkContext) =
        Result(Seq("type", "status", "total"),
            data.issues
                .filter(_.project == "JPN")
                .map(i => (i.typeOf, i.status))
                .countByValue
                .toList
                .sortBy { case (type_status, count) => - count }
                .map { case ((typeOf, status), count) => List(typeOf, status, count) })

}