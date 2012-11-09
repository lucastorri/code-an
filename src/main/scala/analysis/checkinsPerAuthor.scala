package co.torri.dod.analysis

import spark._
import com.thoughtworks.dod._


class CheckinsPerAuthorAnalyzer extends Analyzer {
    def apply(data: RepoData, sc: SparkContext) =
        Result(Seq("author", "checkins"),
            data.commits
                .map(_.author)
                .countByValue
                .toList
                .sortBy { case (author, count) => - count }
                .map(_.productIterator.toList))
}

class ModificationPerAuthorAnalyzer extends Analyzer {

    def apply(data: RepoData, sc: SparkContext) =
        Result(Seq("author", "added", "removed"),
            data.commits
            .flatMap(c => c.files.map(f => (c.author, f.added, f.removed)))
            .groupBy { case (author, added, removed) => author }
            .map { case (author, changes) =>
                val (added, removed) = changes
                    .foldLeft((0,0)) { case ((totalAdded, totalRemoved), (author, added, removed)) =>
                        (totalAdded + added, totalRemoved + removed)
                    }
                (author, added, removed)
            }
            .toArray
            .sortBy { case (author, added, removed) => - (added + removed) }
            .map(_.productIterator.toList))

}