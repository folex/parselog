package com.example

import scala.collection.mutable.ArrayBuffer
import scala.util.Try

object DAO {

  import scalikejdbc._

  Class.forName("org.sqlite.JDBC")

  ConnectionPool.singleton("jdbc:sqlite:parselog.db", null, null)

  private implicit val session = AutoSession

  def init(shouldParse: Boolean) = {
    GlobalSettings.loggingSQLAndTime = GlobalSettings.loggingSQLAndTime.copy(enabled = true, singleLineMode = true, warningEnabled = true)

    sql"""PRAGMA foreign_keys = on""".execute().apply()
    if (shouldParse) {
      sql"""drop table if exists executionLines""".execute().apply()
      sql"""drop table if exists executions""".execute().apply()
    }
    sql"""create table if not exists executions(trace_id TEXT PRIMARY KEY, start_time INTEGER, end_time INTEGER)""".execute().apply()
    sql"""create table if not exists executionLines(id INT PRIMARY KEY, trace_id STRING, line TEXT, FOREIGN KEY (trace_id) REFERENCES executions(trace_id))""".execute().apply()
  }

  case class Execution(traceId: String, startTime: Long, endTime: Long, lines: ArrayBuffer[String] = ArrayBuffer.empty) {
    private val lc = LineTable.column
    private val ec = ExecutionTable.column
    private val threshold = 2000

    def save() = withSQL {
      insert.into(ExecutionTable).values(traceId, startTime, endTime)
    }.update().apply()

    def addLine(line: String, last: Boolean = false) = {
      lines += line
      val size = lines.size
      if (last || size > threshold) {
        if (size > threshold) Main.log.info(s"Threshold was reached for $traceId with size $size")
        withSQL {
          insert into LineTable columns(lc.traceId, lc.line) values(sqls.?, sqls.?)
        }.batch(lines.map(l => Seq(traceId, l)): _*).apply()

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

    private val e = syntax

    def all(limit: Option[Int] = None): List[Execution] = withSQL {
      select.all.from(ExecutionTable as e).limit(limit.getOrElse(-1))
    }.map(wrs => apply(e.resultName)(wrs)).list().apply()

    def apply(s: SyntaxProvider[Execution])(rs: WrappedResultSet): Execution = {
      Execution(rs.get(s.traceId), rs.get(s.startTime), rs.get(s.endTime))
    }

    def apply(s: ResultName[Execution])(rs: WrappedResultSet): Execution = {
      Execution(rs.get(s.traceId), rs.get(s.startTime), rs.get(s.endTime))
    }

  }

  case class Line(traceId: String, line: String)

  object LineTable extends SQLSyntaxSupport[Line] {
    override val tableName = "executionLines"

    val l = syntax
    val lc = column

    def getExecutionLines(traceId: String): List[Line] = {
      val q = withSQL {
        select.from(LineTable as l).where.eq(lc.traceId, traceId)
      }

      val t = Try(q.map(wrs => apply(l.resultName)(wrs)).list().apply())
      if (t.isFailure) {
        print(s"""Executing " ${q.statement} " with traceId $traceId has lead to failure ${t.failed.get}""")
      }
      t.get
    }

    def apply(s: SyntaxProvider[Line])(rs: WrappedResultSet): Line = {
      val traceId: String = rs.get(s.traceId)
      val line: String = rs.get(s.line)
      Line(traceId, line)
    }
    def apply(s: ResultName[Line])(rs: WrappedResultSet): Line = {
      val traceId: String = rs.get(s.traceId)
      val line: String = rs.get(s.line)
      Line(traceId, line)
    }
  }
}
