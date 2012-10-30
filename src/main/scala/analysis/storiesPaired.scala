package co.torri.dod.analysis

import spark._
import com.thoughtworks.dod._


class StoriesPairedAnalyzer extends Analyzer {
    val desc = "Commiter Work Mode per Project"

    def apply(data: RepoData, sc: SparkContext) = {
        val blinkFixedStories = data.issues
            .filter(issue => issue.project == "BLINK" && issue.status == "Fixed")

        val pairedStories = blinkFixedStories
            .filter(issue => issue.devs.map(devs => devs.size).orElse(Some(0)).map(_ >= 2).get)
            .flatMap(issue => issue.devs.get.map(dev => (dev, issue)))
            .groupBy { case (dev, issue) => dev }
            .map { case (dev, stories) => (dev, stories.size) }

        val workedAlone = data.issues
            .filter(issue => issue.devs.map(devs => devs.size).orElse(Some(0)).map(_ == 1).get)
            .groupBy(issue => issue.devs.get.head)
            .map { case (dev, issues) => (dev, issues.size) }

        val d = pairedStories.cartesian(workedAlone)
            .filter { case ((pairDev, nstoriesPaired), (singleDev, nstoriesAlone)) => pairDev == singleDev }
            .map { case ((pairDev, nstoriesPaired), (singleDev, nstoriesAlone)) => (pairDev, nstoriesPaired, nstoriesAlone) }
            .toArray
            .sortBy { case (dev, nstoriesPaired, nstoriesAlone) => dev }
            .map { case (dev, nstoriesPaired, nstoriesAlone) => List(dev, nstoriesPaired, nstoriesAlone) }

        Result(Seq("user", "# paired stories", "# single played stories"), d)
    }

}