package co.torri.dod.analysis

import spark._
import com.thoughtworks.dod._


class CommitsPerWorkspaceAnalyzer extends Analyzer {
    val desc = "Checkins / Workspace"

    def apply(data: RepoData, sc: SparkContext) =
        Result(Seq("workspace", "checkins"),
            data.commits
                .flatMap { commit =>
                    commit.files.map(_.workspace).distinct
                }.countByValue
                .toList
                .sortBy { case (workspace, count) => - count }
                .map { case (workspace, count) => List(workspace.getOrElse("-unknown-"), count) })

}
