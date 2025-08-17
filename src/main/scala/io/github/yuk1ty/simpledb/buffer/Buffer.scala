package io.github.yuk1ty.simpledb.buffer

import io.github.yuk1ty.simpledb.file.{BlockId, FileMgr, Page}
import io.github.yuk1ty.simpledb.log.LogMgr

opaque type TxNum = Int

object TxNum {
  val Unmodified: TxNum = -1

  // TODO: check if non-negative
  def apply(n: Int): TxNum = n match {
    case Unmodified => Unmodified
    case _ => n
  }
}

extension (n: TxNum) {
  def >= (other: Int): Boolean = {
    n >= 0 && other >= 0 && n > other
  }
}

opaque type Lsn = Int

object Lsn {
  val NotGenerated: Lsn = -1

  // TODO: check if non-negative
  def apply(n: Int): Lsn = n match {
    case NotGenerated => NotGenerated
    case _ => n
  }
}

extension (n: Lsn) {
  def unwrap: Int = n
}

object Buffer {
  def apply(fm: FileMgr, lm: LogMgr): Buffer = {
    val contents = Page(fm.blockSize)
    new Buffer(fm, lm, contents, None, 0, TxNum.Unmodified, Lsn.NotGenerated)
  }
}

class Buffer(val fm: FileMgr, val lm: LogMgr, val contents: Page, private var blk: Option[BlockId], private var pins: Int, private var txnum: TxNum, private var lsn: Lsn) {
  def setModified(txNum: Int, lsn: Int): Unit = {
    this.txnum = TxNum(txNum)
    this.lsn = Lsn(lsn)
  }

  def isPinned: Boolean = {
    pins > 0
  }

  def modifyingTx: TxNum = {
    txnum
  }
  
  def block: Option[BlockId] = {
    blk
  }

  def assignToBlock(b: BlockId): Either[RuntimeException, Unit] = {
    flush()
    blk = Some(b)
    for {
      _ <- fm.read(b, contents)
    } yield {
      pins = 0
    }
  }

  def flush(): Either[RuntimeException, Unit] = {
    if (txnum >= 0) {
      for {
        _ <- lm.flush(lsn.unwrap)
        _ <- blk match {
          case Some(b) => fm.write(b, contents)
          case None => Right(())
        }
      } yield {
        txnum = TxNum.Unmodified
      }
    } else {
      Right(())
    }
  }

  def pin(): Unit = {
    pins += 1
  }

  def unpin(): Unit = {
    pins -= 1
  }
}
