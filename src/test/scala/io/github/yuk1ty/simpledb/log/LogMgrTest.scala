package io.github.yuk1ty.simpledb.log

import java.io.File
import io.github.yuk1ty.simpledb.file.{FileMgr, Page}

import java.nio.file.Files
import scala.collection.mutable

class LogMgrTest extends munit.FunSuite {

  override def beforeEach(context: BeforeEach): Unit = {
    val file = File("./data/logtest")
    if (!file.exists()) {
      file.getParentFile.mkdirs()
      Files.createFile(file.toPath)
    }
  }

  override def afterEach(context: AfterEach): Unit = {
    val file = File("./data/logtest")
    Files.deleteIfExists(file.toPath)
  }

  test("should print specific text correctly") {
    // Given
    val lm = LogMgr(FileMgr(File("./data"), 400), "logtest").toTry.get
    val message = "The log file now has these records:"
    def createRecords(start: Int, end: Int): Unit = {
      for (i <- start to end) {
        val rec = createLogRecord(s"record${i}", i + 100)
        lm.append(rec)
      }
    }
    def createLogRecord(s: String, n: Int): Array[Byte] = {
      val npos = Page.maxLength(s.length)
      val b = new Array[Byte](npos + Integer.BYTES)
      val p = Page(b)
      p.setString(0, s)
      p.setInt(npos, n)
      b
    }
    def printLogRecords(msg: String): List[String] = {
      val iter = lm.iterator().toTry.get
      val ans = mutable.ListBuffer[String]()
      while (iter.hasNext) {
        val rec = iter.next()
        val p = Page(rec)
        val s = p.getString(0)
        val npos = Page.maxLength(s.length)
        val value = p.getInt(npos)
        ans += s"[$s, $value]"
      }
      ans.toList
    }

    // When
    createRecords(1, 35)
    val first = printLogRecords(message)
    createRecords(36, 70)
    lm.flush(65).toTry.get
    val second = printLogRecords(message)

    // Then
    assertEquals(first.size, 35)
    assertEquals(second.size, 70)
  }
}
