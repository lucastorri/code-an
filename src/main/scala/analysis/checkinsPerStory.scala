package co.torri.dod.analysis

import spark._
import com.thoughtworks.dod._


class CheckinsPerStoryAnalyzer extends Analyzer {
    def apply(data: RepoData, sc: SparkContext) = {
        val project = "JPN"

        val jpnDefects = data.issues
            .filter(i => i.typeOf == "Defect" && i.project == project && i.status == "Fixed")

        val defectsWithCheckins = data.commits
            .filter(c => c.project.map(_ == project).getOrElse(false))
            .groupBy(c => c.story.getOrElse(0))
            .cartesian(jpnDefects)
            .filter { case ((story, commits), i) => story == i.story }
            .map { case ((story, commits), i) => (i.id, commits.size) }
            .toArray
            .toMap


        val defectsWithNoCheckins = jpnDefects.toArray
            .filterNot(d => defectsWithCheckins.contains(d.id))
            .map(d => (d.id, 0))

        val defectsAndCheckins = (defectsWithCheckins.toList ++ defectsWithNoCheckins)
            .sortBy { case (id, checkins) => - checkins }
            .map(_.productIterator.toList)

        Result(Seq("story", "checkins"),
            defectsAndCheckins)

    }

}