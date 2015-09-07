package com.example

import java.io.{FileOutputStream, File}
import java.nio.charset.Charset
import java.nio.file.{Files, Paths}
import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import com.example.DAO._
import com.google.re2j._
import org.slf4j.LoggerFactory

import scala.annotation.tailrec
import scala.collection.mutable.ArrayBuffer
import scala.util.Try

import scala.sys.process._

object Main extends scala.App {

  import parselog._

  val log = LoggerFactory.getLogger("main")
  log.info("Started")

  if (args.length < 1) {
    println(s"Usage: " +
      s"\nprogramname parse filename" +
      s"\nparses passed file and renders svg for each execution" +
      s"\nprogramname rendersvg [limit]" +
      s"\nrenders first limit executions to svg" +
      s"\nprogramname calctimes filename [outdir]" +
      s"\nparses passed file and renders csv for each execution")
    System.exit(1)
  } else {
    val command = args(0)
    command match {
      case "parse" =>
        DAO.init(true)
        val fileName = args(1)
        parseAndRender(Some(fileName), None)
      case "rendersvg" =>
        DAO.init(false)
        val limit = Try(args(1).toInt).toOption
        parseAndRender(None, limit)
      case "calctimes" =>
        val fileName = args(1)
        val outdir = Try(args(2)).toOption.getOrElse("./csvs/")
        calcTimesAndRender(fileName, outdir)
    }
  }

  def parseAndRender(fileName: Option[String], limit: Option[Int]) = {
    if (fileName.isDefined) {
      parse(fileName.get)
    }
    val trees = getTreeFromDAO(limit)

    val flamestacksPath = "./flamestacks/"
    val flamestacks = new File(flamestacksPath)
    if (!flamestacks.exists()) {
      flamestacks.mkdir()
    }

    trees.foreach { case (traceId, tree) =>
      val file = new File(flamestacksPath + traceId)
      file.createNewFile()
      val stream = new FileOutputStream(file)
      printTreeToFlame(tree, stream)
      stream.close()
    }

    val svgs = trees.unzip._1.map { traceId =>
      traceId -> {
        s"perl ./flamegraph.pl ./flamestacks/$traceId" !!
      }
    }

    val svgsPath = "./svgs/"
    val svgsDir = new File(svgsPath)
    if (!svgsDir.exists()) {
      svgsDir.mkdir()
    }

    svgs.foreach { case (traceId, svg) =>
      val file = new File(svgsPath + traceId + ".svg")
      file.createNewFile()
      val stream = new FileOutputStream(file)
      stream.write(svg.getBytes(Charset.defaultCharset()))
      stream.close()
    }
  }

  def calcTimesAndRender(fileName: String, csvsPath: String) = {
    val times = calculateExecutionTimes(fileName)
    renderTimesToCsv(csvsPath, times)
  }

  log.info(s"Finished")
}

object parselog {
  import Main.log

  val startedRegexPattern = Pattern.compile( """.*?TRACE\[(\d+)\].*?STARTED, startTime = (\d+).*$""")
  val traceRegexPattern = Pattern.compile( """^.*?TRACE\[(\d+)\].*$""")
  val totalRegexPattern = Pattern.compile( """.*?TOTAL: (\d+) ms requestName = (\w+).*""")
  //1. Year 2. Month 3. Day 4. Hour 5. Min 6. Sec 7. Total 8. Request name
  val totalWithTimeRegexPattern = Pattern.compile(""".*?\[([\d]{4})([\d]{2})([\d]{2})-([\d]{2}):([\d]{2}):([\d]{2})\.[\d]{3}\].*?TOTAL: (\d+) ms.*?requestName = (\w+).*""")
  //1. Time offset 2. Name 3. Status 4. Id 5. Total
  val futureNameRegexPattern = Pattern.compile( """.*? time (\d+) ms, ## (.*?) DE future completed with (\w+): \(id: (\d+)\).*?total: (\d+) ms""")

  def parse(fileName: String): Unit = {
    val lineMap = collection.mutable.Map[String, Execution]()
    val lines = scala.io.Source.fromFile(fileName).getLines()
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

  case class Request(name: String, start: Long, end: Long = -1, total: Long = -1)
  // 1. Year 2. Month 3. Day 4. Hour 5. Min 6. Sec 7. Millis 8. Connection Id 9. REQ or RES 10. Request name
  val requestPattern = Pattern.compile( """\S{3}\s\[(\d{4})(\d{2})(\d{2})-(\d{2}):(\d{2}):(\d{2})\.(\d{3})\]\s\S+\s\(\S*\):\s(\S+)\s(\S+):\s(\w+).*""")

  def calculateExecutionTimes(fileName: String): Map[String, Seq[(Long, Long)]] = {
    val lines = scala.io.Source.fromFile(fileName).getLines()
    val cal = Calendar.getInstance()

    val requests = lines.foldLeft(Map.empty[String, Seq[Request]]) { case (map, line) =>
      val m = requestPattern.matcher(line)
      if (!m.find()) {
        log.error(s"Can't match $line")
        map
      } else {
        val year = m.group(1).toInt
        val month = m.group(2).toInt - 1 //0 based
        val day = m.group(3).toInt
        val hour = m.group(4).toInt
        val min = m.group(5).toInt
        val sec = m.group(6).toInt
        val millis = m.group(7).toInt
        val connectionId = m.group(8)
        val lineType = m.group(9)
        val requestName = m.group(10)

        cal.set(year, month, day, hour, min, sec)
        cal.set(Calendar.MILLISECOND, millis)
        val unixTime = cal.getTimeInMillis

        if (lineType == "REQ") {
          if (requestName == "Ping") map
          else {
            val request = Request(requestName, unixTime)
            map.updated(connectionId, request +: map.getOrElse(connectionId, Seq.empty))
          }
        } else if (lineType == "RES") {
          val rName = {
            if (requestName == "Pong") ""
            else if (requestName.endsWith("Response")) {
              requestName.dropRight(8) + "Request"
            } else {
              log.info(s"Strange requestName $requestName)")
              ""
            }
          }
          if (rName.nonEmpty) {
            val requests = map.get(connectionId)
            val (before, withAndAfter) = requests.getOrElse(Seq.empty).span(r => !(r.end == -1 && r.name == rName))
            val request = withAndAfter.headOption
            request.fold {
              log.info(s"Unable to find REQ for $rName for $connectionId")
              map
            } { r =>
              val filledReq = r.copy(end = unixTime, total = unixTime - r.start)
              if (filledReq.total < 0) {
                log.info(s"Got negative total for $connectionId $rName. Was $r became $filledReq")
              }
              val reqs = (before :+ filledReq) ++ withAndAfter.drop(1)
              map.updated(connectionId, reqs)
            }
          } else map
        } else {
          throw new RuntimeException(s"Incorrect line type on line $line")
        }

      }
    }
    val res = requests.values.flatten.filter(_.end > 0).groupBy(_.name).mapValues(_.toSeq.sortBy(_.end).map(r => r.end/1000 -> r.total))
    res
  }

  def renderTimesToCsv(csvsPath: String, times: Map[String, Seq[(Long, Long)]]) = {
    val csvsDir = new File(csvsPath)
    val path = csvsPath + new SimpleDateFormat("YMMdd_HHmm").format(Calendar.getInstance.getTime)
    val subdir = new File(path)
    if (!csvsDir.exists()) {
      csvsDir.mkdir()
    }
    if (!subdir.exists()) {
      subdir.mkdir()
    }

    times.foreach { case (name, series) =>
      val p = path + "/" + name.toLowerCase + ".csv"
      log.info(s"Creating $p")
      val file = new File(p)
      file.createNewFile()
      val stream = new FileOutputStream(file)
      series.foreach { case (time, total) =>
        stream.write(s"$time,$total\n".getBytes(Charset.defaultCharset()))
      }
      stream.close()
    }
  }

  def getTreeFromDAO(limit: Option[Int] = None) = {
    val executions = ExecutionTable.all(limit = limit)
    val filledExecutions = executions.map(e => e.copy(lines = ArrayBuffer(e.getLines.map(_.line): _*))).filter(_.lines.nonEmpty)
    filledExecutions.map(fe => fe.traceId -> parseExecutionToTree(fe))
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

        val endTime = execution.startTime + timeOffset
        val startTime = endTime - totalTime

        Some(FutureInfo(id, startTime, endTime, futureName, status, totalTime, timeOffset))
      } else None
    }

    val sorted = infos.sortBy(fi => (-fi.total, fi.endTime))
    val initial = infos.maxBy(fi => (fi.total, fi.endTime))

    /*
     * We always assume that remainingInfos are sorted by `(fi => (fi.startTime - fi.endTime, fi.endTime))`
     */
    def buildTree(root: FutureInfo, remainingInfos: List[FutureInfo], level: Int, parent: Int): List[FutureNode] = {
      @tailrec
      def findNodes(rem: List[FutureInfo], total: Long = 0, neighbors: List[FutureInfo] = List.empty): List[FutureInfo] = {
        if (total < root.total && rem.nonEmpty) {
          val neighbor = rem.minBy(fi => (fi.startTime, -fi.total))
          val newRem = rem.filter(fi => fi.startTime >= neighbor.endTime && fi.endTime <= root.endTime && fi.id != neighbor.id)
          findNodes(rem = newRem, total = total + neighbor.total, neighbors = neighbor +: neighbors)
        } else {
          neighbors
        }
      }

      val neighbors = findNodes(remainingInfos)

      val remaining = remainingInfos.filterNot(fi => neighbors.exists(_.id == fi.id))

      neighbors.zipWithIndex.map { case (n, ni) =>
        val rems = remaining.filter(fi => fi.startTime >= n.startTime && fi.endTime <= n.endTime)
        FutureNode(n, buildTree(n, rems, level + 1, ni))
      }
    }

    FutureNode(initial, buildTree(initial, sorted, 0, -1))
  }

  def printTreeToFlame(tree: FutureNode, file: FileOutputStream): Unit = {
    def helper(node: FutureNode, prefix: String = ""): Unit = {
      file.write((s"$prefix${node.info.name} ${node.info.total}\n").getBytes(Charset.defaultCharset()))
      node.nodes.foreach { n =>
        helper(n, prefix + node.info.name + ";")
      }
    }

    helper(tree)
  }

  case class FutureTime(success: Long = 0L, failure: Long = 0L) {
    def +(time: FutureTime) = FutureTime(success + time.success, failure + time.failure)
  }
  case class FutureNode(info: FutureInfo, nodes: List[FutureNode])
  case class FutureInfo(id: Long, startTime: Long, endTime: Long, name: String, status: String, total: Long, timeOffset: Long)
}