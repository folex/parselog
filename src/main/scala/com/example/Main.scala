package com.example

import org.slf4j.LoggerFactory
import scala.collection.mutable

object Main extends scala.App {
  db.init()

  import parselog._
  val log = LoggerFactory.getLogger("main")
  log.info("Started")
  val parsedLog = parse("small.log")
  log.info(s"Finished")
  //    val data = makeHistData(parsedLog)
  //    println(data.toSeq.sortBy(_._2.success).map { case (name, time) => s"$name\t\t${time.success}" }.mkString("\n"))
}

object parselog {

  import com.google.re2j._
  import db.Execution

  val startedRegexPattern = Pattern.compile( """.*?TRACE\[(\d+)\].*?STARTED, startTime = (\d+).*$""")
  val traceRegexPattern = Pattern.compile( """^.*?TRACE\[(\d+)\].*$""")
  val totalRegexPattern = Pattern.compile( """.*?TOTAL: (\d+) ms requestName = (\w+).*""")
  val futureNameRegexPattern = Pattern.compile( """.*?## (.*?) DE future completed with (\w+).*?total: (\d+) ms""")

  def parse(fileName: String): Unit = {
    val lineMap = collection.mutable.Map[String, Execution]()
    val lines = scala.io.Source.fromFile("/Users/folex/Development/logs/" + fileName).getLines()
    lines.foreach(line =>
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
    )
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
  //          val total = m.group(3).toLong
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

  case class FutureNode(name: String, start: Long, end: Long, nodes: List[FutureNode])
  def parseToTree(fileName: String) = {
    val parsed = parse(fileName)

  }
}

object db {

  import scalikejdbc._

  Class.forName("org.sqlite.JDBC")

  ConnectionPool.singleton("jdbc:sqlite:parselog.db", null, null)

  private implicit val session = AutoSession

  case class Execution(traceId: String, startTime: Long, endTime: Long) {
    private val lc = LineTable.column
    private val ec = ExecutionTable.column
    private var lines = mutable.ArrayBuffer.empty[String]

    def save() = withSQL {
      insert.into(ExecutionTable).values(traceId, startTime, endTime)
    }.update().apply()

    def addLine(line: String, last: Boolean = false) = {
      lines += line
      if (last) {
        withSQL {
          insert into LineTable columns (lc.traceId, lc.line) values (sqls.?, sqls.?)
        }.batch(lines.map(l => Seq(traceId, l)) : _*).apply()

        lines.clear()
      }
    }

    def modify() = withSQL {
      update(ExecutionTable).set(
        ec.startTime -> startTime,
        ec.endTime -> endTime
      ).where.eq(ec.traceId, traceId)
    }.update().apply()

    def getLines = LineTable.getExecutionLines(traceId)
  }

  object ExecutionTable extends SQLSyntaxSupport[Execution] {
    override val tableName = "executions"

    private val e = ExecutionTable.syntax("e")

    def apply(s: SyntaxProvider[Execution])(rs: WrappedResultSet): Execution = Execution(rs.get(s.traceId), rs.get(s.startTime), rs.get(s.endTime))

    def all() = withSQL {
      select.all.from(ExecutionTable as e)
    }.map(wrs => apply(e)(wrs)).list().apply()
  }


  case class Line(id: Int, traceId: String, line: String)
  object LineTable extends SQLSyntaxSupport[Line] {
    override val tableName = "executionLines"

    val l = syntax("l")
    val lc = column

    def apply(s: SyntaxProvider[Line])(rs: WrappedResultSet): Line = Line(rs.get(s.id), rs.get(s.traceId), rs.get(s.line))

    def getExecutionLines(traceId: String) = withSQL {
      select.from(LineTable as l).where.eq(lc.traceId, traceId)
    }.map(wrs => apply(l)(wrs))
  }

  def init() = {
    GlobalSettings.loggingSQLAndTime = GlobalSettings.loggingSQLAndTime.copy(enabled = false, singleLineMode = true, warningEnabled = true)

    sql"""PRAGMA foreign_keys = on""".execute().apply()
    sql"""create table if not exists executions(trace_id TEXT PRIMARY KEY, start_time INTEGER, end_time INTEGER)""".execute().apply()
    sql"""create table if not exists executionLines(id INT PRIMARY KEY, trace_id STRING, line TEXT, FOREIGN KEY (trace_id) REFERENCES executions(trace_id))""".execute().apply()
  }
}
