package io.github.yuk1ty.simpledb.log

import io.github.yuk1ty.simpledb.file.{BlockId, FileMgr, Page}

object LogMgr {
  def apply(fm: FileMgr, logFile: String): Either[RuntimeException, LogMgr] = {
    val b = new Array[Byte](fm.blockSize)
    val logPage = Page(b)
    for {
      logSize <- fm.length(logFile)
      currentblk <-
        if (logSize == 0) {
          appendNewBlock(fm, logFile, logPage)
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

private def appendNewBlock(
    fm: FileMgr,
    logFile: String,
    logPage: Page
): Either[RuntimeException, BlockId] = {
  for {
    blk <- fm.append(logFile)
    _ = logPage.setInt(0, fm.blockSize)
    _ <- fm.write(blk, logPage)
  } yield {
    blk
  }
}

class LogMgr(
    val fm: FileMgr,
    val logFile: String,
    val logPage: Page,
    private var currentblk: BlockId,
    private var latestLSN: Int,
    private var lastSavedLSN: Int
) {
  def flush(lsn: Int): Either[RuntimeException, Unit] = {
    if (lsn < 0) {
      return Right(())
    }
    flush()
  }

  def flush(): Either[RuntimeException, Unit] = {
    fm.write(currentblk, logPage)
    lastSavedLSN = latestLSN
    Right(())
  }

  def append(logRec: Array[Byte]): Int = synchronized {
    var boundary = logPage.getInt(0)
    val recSize = logRec.length
    val bytesNeeded = recSize + Integer.BYTES
    if (boundary - bytesNeeded < Integer.BYTES) {
      flush()
      appendNewBlock(fm, logFile, logPage).foreach { blk =>
        currentblk = blk
      }
      boundary = logPage.getInt(0)
    }
    val recPos = boundary - bytesNeeded
    logPage.setBytes(recPos, logRec)
    logPage.setInt(0, recPos)
    latestLSN += 1
    latestLSN
  }

  def iterator(): Either[RuntimeException, Iterator[Array[Byte]]] = {
    for {
      _ <- flush()
      iter <- LogIterator(fm, currentblk)
    } yield iter
  }
}

