package io.github.yuk1ty.simpledb.log

import io.github.yuk1ty.simpledb.file.{BlockId, FileMgr, Page}

private[log] object LogIterator {
  inline def apply(fm: FileMgr, blk: BlockId): Either[RuntimeException, LogIterator] = {
    val b = new Array[Byte](fm.blockSize)
    val p = Page(b)
    val iter = new LogIterator(fm, blk, p, 0, 0)
    for {
      _ <- iter.moveToBlock(blk)
    } yield iter
  }
}

private[log] class LogIterator(val fm: FileMgr, private var blk: BlockId, private val p: Page, private var currentPos: Int, private var boundary: Int) extends Iterator[Array[Byte]] {
  def hasNext: Boolean = currentPos < fm.blockSize || blk.blknum > 0

  def next(): Array[Byte] = {
    // Deliberately throwing an exception to keep the functional signature as `next(): Array[Byte]`.
    (for {
      _ <- if (currentPos == fm.blockSize) {
        blk = BlockId(blk.fileName, blk.blknum - 1)
        moveToBlock(blk)
      } else {
        Right(())
      }
    } yield {
      val rec = p.getBytes(currentPos)
      currentPos += Integer.BYTES + rec.length
      rec
    }).toTry.get
  }

  private def moveToBlock(blk: BlockId): Either[RuntimeException, Unit] = {
    for {
      _ <- fm.read(blk, p)
    } yield {
      boundary = p.getInt(0)
      currentPos = boundary
    }
  }
}
