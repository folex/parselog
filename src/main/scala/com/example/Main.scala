package com.example

import org.slf4j.LoggerFactory
import scala.annotation.tailrec
import scala.collection.concurrent.TrieMap
import scala.collection.mutable.ArrayBuffer
import com.google.re2j._
import DAO._

import scala.util.Try

object Main extends scala.App {
  DAO.init()

  import parselog._
  val log = LoggerFactory.getLogger("main")
  log.info("Started")
//  parse("filtered.log")
  val trees = getTreeFromDAO(Some(2))

  log.info(s"Finished")
  //    val data = makeHistData(parsedLog)
  //    println(data.toSeq.sortBy(_._2.success).map { case (name, time) => s"$name\t\t${time.success}" }.mkString("\n"))
}

object parselog {
  val startedRegexPattern = Pattern.compile( """.*?TRACE\[(\d+)\].*?STARTED, startTime = (\d+).*$""")
  val traceRegexPattern = Pattern.compile( """^.*?TRACE\[(\d+)\].*$""")
  val totalRegexPattern = Pattern.compile( """.*?TOTAL: (\d+) ms requestName = (\w+).*""")
  //1. Time offset 2. Name 3. Status 4. Id 5. Total
  val futureNameRegexPattern = Pattern.compile( """.*? time (\d+) ms, ## (.*?) DE future completed with (\w+): \(id: (\d+)\).*?total: (\d+) ms""")

  def parse(fileName: String): Unit = {
    val lineMap = collection.mutable.Map[String, Execution]()
    val lines = scala.io.Source.fromFile("/Users/folex/Development/logs/" + fileName).getLines()
    lines.foreach { line =>
      if (startedRegexPattern.matches(line)) {
        val m = startedRegexPattern.matcher(line)
        m.find()
        val ex = Execution(m.group(1), m.group(2).toLong, -1L)
        ex.save()
        lineMap.update(m.group(1), ex)
      } else if (traceRegexPattern.matches(line)) {
        val rm = traceRegexPattern.matcher(line)
        rm.find()
        val id = rm.group(1)
        if (lineMap.contains(id)) {
          if (line.contains("FINISHED")) {
            val tm = totalRegexPattern.matcher(line)
            tm.find()
            val oldEx = lineMap(id)
            val ex = oldEx.copy(endTime = oldEx.startTime + tm.group(1).toLong)
            ex.modify()
            ex.addLine(line, last = true)
          } else {
            val ex = lineMap(id)
            ex.addLine(line)
          }
        }
      }
    }
  }

  case class FutureTime(success: Long = 0L, failure: Long = 0L) {
    def +(time: FutureTime) = FutureTime(success + time.success, failure + time.failure)
  }

  //  def makeHistData(traceIdToLines: collection.Map[String, Execution]) = {
  //    val nameToLines = traceIdToLines.toSeq.par.flatMap { case (traceId, execution) =>
  //      if (totalRegexPattern.matches(execution.lines.last)) {
  //        val m = totalRegexPattern.matcher(execution.lines.last)
  //        m.find()
  //        Some(m.group(2) -> execution.lines)
  //      } else None
  //    }
  //    val nameToFutureNameToTime = nameToLines.map { case (requestName, lines) =>
  //      requestName -> lines.map { l =>
  //        if (futureNameRegexPattern.matches(l)) {
  //          val m = futureNameRegexPattern.matcher(l)
  //          m.find()
  //          val futureName = m.group(1)
  //          val status = m.group(2)
  //          val total = m.group(4).toLong
  //          Some(futureName -> (status match {
  //            case "success" => FutureTime(success = total)
  //            case "failure" => FutureTime(failure = total)
  //          }))
  //        } else None
  //      }
  //    }
  //
  //    val futureNameToTime = nameToFutureNameToTime.unzip._2.flatten.flatten
  //    val summedFutureNameToTime = futureNameToTime.foldLeft(Map[String, FutureTime]()) { case (map, (name, time)) =>
  //      map.updated(name, map.getOrElse(name, FutureTime()) + time)
  //    }
  //    summedFutureNameToTime
  //  }

  case class FutureNode(info: FutureInfo, nodes: List[FutureNode])
  case class FutureInfo(id: Long, startTime: Long, endTime: Long, name: String, status: String, total: Long)

  def getTreeFromDAO(limit: Option[Int] = None) = {
    val executions = ExecutionTable.all(limit = limit)
    val filledExecutions = executions.map(e => e.copy(lines = ArrayBuffer(e.getLines.map(_.line) : _*)))
    filledExecutions.map(parseExecutionToTree)
  }

  def parseExecutionToTree(execution: Execution) = {
    val infos = execution.lines.toList.flatMap { line =>
      if (futureNameRegexPattern.matches(line)) {
        val m = futureNameRegexPattern.matcher(line)
        m.find()
        val timeOffset = m.group(1).toLong
        val futureName = m.group(2)
        val status = m.group(3)
        val id = m.group(4).toLong
        val totalTime = m.group(5).toLong

        val startTime = execution.startTime + timeOffset
        val endTime = startTime + totalTime

        Some(FutureInfo(id, startTime, endTime, futureName, status, totalTime))
      } else None
    }

    val sorted = infos.sortBy(fi => (-fi.total, fi.endTime))
    val initial = infos.maxBy(fi => (fi.total, fi.endTime))

    /*
     * We always assume that remainingInfos are sorted by `(fi => (fi.startTime - fi.endTime, fi.endTime))`
     */
    def buildTree(root: FutureInfo, remainingInfos: List[FutureInfo]): List[FutureNode] = {
//      var total: Int = 0
//      var rem = remainingInfos
//      var neighbors = List.empty[FutureInfo]
//      while (total < root.total) {
//        val neighbor = rem.maxBy(fi => (fi.total, -fi.startTime))
//        neighbors +:= neighbor
//        total += neighbor.total
//        rem = rem.filter(_.startTime >= neighbor.endTime)
//      }

      @tailrec
      def findNodes(rem: List[FutureInfo], total: Long = 0, neighbors: List[FutureInfo] = List.empty): List[FutureInfo] = {
        if (total < root.total) {
          val neighbor = rem.maxBy(fi => (fi.total, -fi.startTime))
          val newRem = rem.filter(_.startTime >= neighbor.endTime)
          findNodes(newRem, total + neighbor.total, neighbor +: neighbors)
        } else {
          neighbors
        }
      }

      val neighbors = findNodes(remainingInfos)

      val remaining = remainingInfos.filterNot(fi => neighbors.exists(_.id == fi.id))

      neighbors.map(n => FutureNode(n, buildTree(n, remaining)))
    }

    FutureNode(initial, buildTree(initial, sorted))
  }
}