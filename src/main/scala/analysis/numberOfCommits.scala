package co.torri.dod.analysis

import spark._
import com.thoughtworks.dod._


@Deactivated
class NumberOfCommitsAnalyzer extends Analyzer {
    val desc = "Total Checkins"

    def apply(data: RepoData, sc: SparkContext) =
        Result(Seq("checkins"), Seq(Seq(data.commits.count)))

}