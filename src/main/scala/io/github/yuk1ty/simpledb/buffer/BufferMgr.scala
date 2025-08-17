package io.github.yuk1ty.simpledb.buffer

import io.github.yuk1ty.simpledb.buffer.BufferMgr.MaxTime
import io.github.yuk1ty.simpledb.file.{BlockId, FileMgr}
import io.github.yuk1ty.simpledb.log.LogMgr

import scala.util.Try

object BufferMgr {
  private val MaxTime: Long = 10000 // 10 seconds

  def apply(fm: FileMgr, lm: LogMgr, numbuffs: Int): BufferMgr = {
    val bufferPool = Array.fill(numbuffs)(Buffer(fm, lm))
    val numAvailable = numbuffs
    new BufferMgr(bufferPool, numAvailable)
  }
}

class BufferMgr(val bufferPool: Array[Buffer], var numAvailable: Int) {
  def available(): Int = synchronized {
    numAvailable
  }

  def flushAll(txnum: TxNum): Either[RuntimeException, Unit] = synchronized {
    for (buff <- bufferPool) {
      if (buff.modifyingTx == txnum) {
        buff.flush()
      }
    }
    Right(())
  }

  def unpin(buff: Buffer): Unit = synchronized {
    buff.unpin()
    if (!buff.isPinned) {
      numAvailable += 1
      notifyAll()
    }
  }

  def pin(blk: BlockId): Either[RuntimeException, Option[Buffer]] = synchronized {
    val timestamp = System.currentTimeMillis()
    // TODO: we can make here as purity functions
    var buff = tryToPin(blk)
    buff match {
      case Left(e) => Left(e)
      case Right(b) =>
        while (b.isEmpty && !waitingTooLong(timestamp)) {
          try {
            wait(MaxTime)
          } catch {
            case e: InterruptedException =>
              return Left(new BufferAbortException(s"Cannot pin block $blk after waiting for too long."))
          }
          buff = tryToPin(blk)
        }
        b match {
          case b@Some(_) => Right(b)
          case None => Left(new BufferAbortException(s"Cannot pin block $blk after waiting for too long."))
        }
    }
  }

  private def waitingTooLong(starttime: Long): Boolean = System.currentTimeMillis() - starttime > MaxTime

  private def tryToPin(blk: BlockId): Either[RuntimeException, Option[Buffer]] = {
    findExistingBuffer(blk) match {
      case Some(b) =>
        if (!b.isPinned) {
          numAvailable -= 1
        }
        b.pin()
        Right(Some(b))
      case None =>
        val pinnedBuff = choosePinnedBuffer()
        pinnedBuff match {
          case Some(b) =>
            b.assignToBlock(blk) match {
              case Left(e) => Left(e)
              case Right(_) => Right(Some(b))
            }
          case None => Right(None)
        }
    }
  }

  private def findExistingBuffer(blk: BlockId): Option[Buffer] = {
    bufferPool.find(_.block.exists(_ == blk))
  }

  private def choosePinnedBuffer(): Option[Buffer] = bufferPool.find(!_.isPinned)
}

private[buffer] class BufferAbortException(message: String) extends RuntimeException(message)
