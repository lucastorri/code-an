package co.torri.dod.analysis

import spark._
import com.thoughtworks.dod._


class PairingRateAnalyzer extends Analyzer {
    val desc = "Pairing Rate per Project"

    def apply(data: RepoData, sc: SparkContext) = {
        val storiesPerPairingStatus = data.issues
            .filter(issue => issue.project == "BLINK" && issue.status == "Fixed")
            .groupBy(issue => issue.devs.map(devs => devs.size).orElse(Some(0)).map(_ >= 2).get)
            .map { case (pairing, stories) => (pairing, stories.size) }
            .toArray

        val totalStories = storiesPerPairingStatus.foldLeft(0) { case (total, (status, nstories)) => total + nstories }


        Result(Seq("pairing", "#stories", "%"),
            storiesPerPairingStatus.map { case (status, nstories) =>
                List(status, nstories, (nstories.toDouble * 100) / totalStories) })
    }

}