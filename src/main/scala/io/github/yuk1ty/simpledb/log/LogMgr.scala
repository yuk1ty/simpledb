package io.github.yuk1ty.simpledb.log

import io.github.yuk1ty.simpledb.file.{BlockId, FileMgr, Page}

object LogMgr {
  def apply(fm: FileMgr, logFile: String): Either[RuntimeException, LogMgr] = {
    val b = new Array[Byte](fm.blockSize)
    val logPage = Page(b)
    for {
      logSize <- fm.length(logFile)
      currentblk <- if (logSize == 0) {
        Right(appendNewBlock())
      } else {
        val blk = BlockId(logFile, logSize - 1)
        fm.read(blk, logPage).map(_ => blk).left.map(identity)
      }
    } yield {
      val latestLSN = 0
      val lastSavedLSN = 0
      new LogMgr(fm, logFile, logPage, currentblk, latestLSN, lastSavedLSN)
    }
  }
}

private def appendNewBlock(): BlockId = ???

class LogMgr(val fm: FileMgr, val logFile: String, val logPage: Page, val currentblk: BlockId, private var latestLSN: Int, private var lastSavedLSN: Int) {

}