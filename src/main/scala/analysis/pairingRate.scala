package co.torri.dod.analysis

import spark._
import com.thoughtworks.dod._


class PairingRateAnalyzer extends Analyzer {
    val desc = "Pairing"

    def apply(data: RepoData, sc: SparkContext) = {
        val d = data.issues
            .filter(_.status == "Fixed")
            .map(issue => (issue.project, issue.devs.map(devs => devs.size).orElse(Some(0)).map(_ >= 2).get))
            .groupBy { case (project, pairing) => (project, pairing) }
            .map { case ((project, pairing), stories) => (project, pairing, stories.size) }
            .toArray
            .sortBy { case (project, pairing, nstories) => (project, pairing) }
            .map(_.productIterator.toList)

        Result(Seq("project", "pairing", "nstories"), d)
    }

}