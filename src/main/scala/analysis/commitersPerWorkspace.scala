package co.torri.dod.analysis

import spark._
import com.thoughtworks.dod._


class CommitersPerWorkspaceAnalyzer extends Analyzer {
    def apply(data: RepoData, sc: SparkContext) = {

        val d = data.commits
            .map(c => (c.author, c.files.flatMap(fc => fc.workspace)))
            .flatMap { case (author, workspaces) => workspaces.map(ws => (author, ws)) }
            .groupBy { case (author, ws) => ws }
            .map { case (ws, authors) => (ws, authors.distinct.map(_._1)) }
            .toArray
            .sortBy { case (ws, commiters) => - commiters.size }
            .map { case (ws, commiters) => List(ws, commiters.size, commiters.sorted.mkString(", ")) }

        Result(Seq("workspace", "# commiters", "commiters"), d)
    }
}