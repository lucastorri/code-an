package co.torri.dod.analysis

import spark._
import com.thoughtworks.dod._


@Deactivated
class CommitsPerWorkspacePerProjectAnalyzer extends Analyzer {
    val desc = "Checkins / Workspace / Project"

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
