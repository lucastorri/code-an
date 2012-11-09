package co.torri.dod.analysis

import spark._
import com.thoughtworks.dod._


class NumberOfCommitsAnalyzer extends Analyzer {
    def apply(data: RepoData, sc: SparkContext) =
        Result(Seq("checkins"), Seq(Seq(data.commits.count)))

}