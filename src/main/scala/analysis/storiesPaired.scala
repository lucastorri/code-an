package co.torri.dod.analysis

import spark._
import com.thoughtworks.dod._


class StoriesPairedAnalyzer extends Analyzer {
    val desc = "Commiter Paired Stories per Project"

    def apply(data: RepoData, sc: SparkContext) = {
        val d = data.issues
            .filter(issue => issue.project == "BLINK" && issue.status == "Fixed")
            .filter(issue => issue.devs.map(devs => devs.size).orElse(Some(0)).map(_ >= 2).get)
            .flatMap(issue => issue.devs.get.map(dev => (dev, issue)))
            .groupBy { case (dev, issue) => dev }
            .map { case (dev, stories) => (dev, stories.size) }
            .toArray
            .sortBy { case (dev, nstories) => - nstories }
            .map { case (dev, stories) => List(dev, stories) }

        Result(Seq("user", "# paired stories"), d)
    }

}