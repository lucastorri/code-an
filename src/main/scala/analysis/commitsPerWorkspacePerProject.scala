package co.torri.dod.analysis

import spark._
import com.thoughtworks.dod._


class CommitsPerWorkspacePerProjectAnalyzer extends Analyzer {
    def apply(data: RepoData, sc: SparkContext) =
        Result(Seq("workspace", "project", "checkins"),
            data.commits
                .flatMap { commit =>
                    commit.files
                        .flatMap(_.workspace)
                        .distinct
                        .map((_, commit.project))
                }.countByValue
                .toList
                .sortBy { case (worskpace_project, count) => - count }
                .map{ case ((ws, proj), count) => List(ws, proj.getOrElse("-none-"), count) })
}
