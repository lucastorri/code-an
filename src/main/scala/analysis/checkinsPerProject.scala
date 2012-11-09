package co.torri.dod.analysis

import spark._
import com.thoughtworks.dod._


class CheckinsPerProjectAnalyzer extends Analyzer {
    def apply(data: RepoData, sc: SparkContext) =
        Result(Seq("project", "checkins"),
            data.commits
                .map(_.project)
                .countByValue
                .toList
                .sortBy { case (proj, count) => - count }
                .map { case (proj, count) => List(proj.getOrElse("-none-"), count) })

}
