package co.torri.dod.analysis

import spark._
import com.thoughtworks.dod._


@Deactivated
class CheckinsPerAuthorAnalyzer extends Analyzer {
    val desc = "Number of Checkins / Author"

    def apply(data: RepoData, sc: SparkContext) =
        Result(Seq("author", "checkins"),
            data.commits
                .map(_.author)
                .countByValue
                .toList
                .sortBy { case (author, count) => - count }
                .map(_.productIterator.toList))
}
