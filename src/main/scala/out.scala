package com.thoughtworks.dod.out

import com.thoughtworks.dod._


trait OutputFormatter {
    def export(analyzerDesc: String, r: Result)
}

object sysout extends OutputFormatter {

    val outputSeparator = "="
    val columnSeparator = " | "
    val labelDataSeparator = "-"

    def export(analyzerDesc: String, r: Result) = {
        println(analyzerDesc)
        val sizes = r.labels.map(_.size).toArray
        r.rows.foreach { row =>
            row.zipWithIndex.foreach { case (column, i) => sizes(i) = math.max(sizes(i), column.toString.size) }
        }
        val sepSize = (sizes.sum + ((sizes.size - 1) * columnSeparator.size))
        println(outputSeparator * sepSize)
        println(r.labels.zipWithIndex.map { case (label, i) => label.padTo(sizes(i), ' ') }.mkString(columnSeparator))
        println(labelDataSeparator * sepSize)
        r.rows.foreach { row =>
            println(row.zipWithIndex.map { case (column, i) => column.toString.padTo(sizes(i), ' ') }.mkString(columnSeparator))
        }
        println(outputSeparator * sepSize)
        println()
    }
}