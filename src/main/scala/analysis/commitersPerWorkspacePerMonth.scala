package co.torri.dod.analysis

import spark._
import com.thoughtworks.dod._
import scala.annotation.switch


class CommitersPerWorkspacePerMonthAnalyzer extends Analyzer {
    val desc = "Commiters / Month / Workspace"

    def apply(data: RepoData, sc: SparkContext) = {



        val d = data.commits
            .flatMap { case c => c.files.flatMap(fc => fc.workspace).map(ws => (c.yearMonth, c.author, ws)) }
            .groupBy { case (date, author, ws) => (date, ws) }
            .map { case ((date, ws), i) => ((date, ws), i.distinct.size) }
            .toArray
            .sortBy { case ((date, ws), checkins) => (ws, date) }
            .map { case (((year, month), ws), checkins) => List(ws, year, (month: @switch) match { //XXX ugly as hell, but...
                    case 0 => "Jan"
                    case 1 => "Feb"
                    case 2 => "Mar"
                    case 3 => "Apr"
                    case 4 => "May"
                    case 5 => "Jun"
                    case 6 => "Jul"
                    case 7 => "Aug"
                    case 8 => "Sep"
                    case 9 => "Oct"
                    case 10 => "Nov"
                    case 11 => "Dec"
                }, checkins)
            }


        Result(Seq("workspace", "year", "month", "checkins"), d)
    }
}