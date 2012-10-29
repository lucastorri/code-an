package co.torri.dod.analysis

import spark._
import com.thoughtworks.dod._


@Deactivated
class MostModifiedFilesInBugFixesAnalyzer extends Analyzer {
    val desc = "Most Modified Files In Bug Fixes"

    def apply(data: RepoData, sc: SparkContext) = {
        val project = "JPN"

        val jpnCommits = data.commits
            .filter { c =>
                c.story.nonEmpty && c.project.exists(p => p.equalsIgnoreCase(project))
            }

        val jpnBugs = data.issues
            .filter { i =>
                i.project.equalsIgnoreCase(project) && i.typeOf == "Defect"
            } ++ sc.makeRDD(Seq(Issue("", project, 0, "Defect", "Unresolved", None)), sc.defaultParallelism)

        val count = jpnCommits.cartesian(jpnBugs)
            .filter { case (c, i) => c.story.get == i.story }
            .flatMap{ case (c, i) => c.files.map(_.path) }
            .countByValue
            .toList
            .sortBy { case (file, count) =>  - count }
            .map(_.productIterator.toList)

        Result(Seq("file", "checkins"), count)
    }

}