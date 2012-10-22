package co.torri.dod.analysis

import spark._
import com.thoughtworks.dod._


@Deactivated
class TimesAuthorModifiedAFileAnalyzer extends Analyzer {
    val desc = "Times an author modified a same file"

    def apply(data: RepoData, sc: SparkContext) =
        Result(Seq("file", "author", "checkins"),
            data.commits
                .groupBy(_.author)
                .flatMap { case (author, commits) =>
                    commits.flatMap(_.files).map(f => (f.path, author))
                }.countByValue
                .toList
                .sortBy { case (file_author, count) => - count }
                .map{ case ((f, a), c) => Seq(f, a, c) })

}
