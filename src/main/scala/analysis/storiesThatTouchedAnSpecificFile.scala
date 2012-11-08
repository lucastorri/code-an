package co.torri.dod.analysis

import spark._
import com.thoughtworks.dod._
import scala.annotation.switch


class StoriesThatTouchedSpecificFileshAnalyzer extends Analyzer {
    val desc = "Stories that touched given files"

    def apply(data: RepoData, sc: SparkContext) = {
        val filenames = List(
            "ProductUpdateController",
            "ProductService",
            "ProductDataService"
        )


        val d = data.commits
            .filter(_.id.nonEmpty)
            .flatMap { c =>
                c.files
                    .filter(f => f.workspace.exists(_ == "pacman"))
                    .filter(f => filenames.exists(fn => f.path.contains(fn)))
                    .map(f => (c, f.path))
            }
            .groupBy { case (c, f) => f }
            .map { case (f, commit_file) => (f, commit_file.map { case (c, f) => c.id.get }.distinct) }
            .toArray
            .sortBy { case (f, stories) => f }
            .map { case (f, stories) => List(f, stories.mkString(", ")) }

        Result(Seq("file", "stories"), d)
    }

}