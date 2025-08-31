package io.github.yuk1ty.simpledb.transaction

import io.github.yuk1ty.simpledb.file.FileMgr
import io.github.yuk1ty.simpledb.log.LogMgr
import io.github.yuk1ty.simpledb.buffer.BufferMgr
import io.github.yuk1ty.simpledb.file.BlockId

case class Transaction(fm: FileMgr, lm: LogMgr, bm: BufferMgr) {
  def commit(): Unit = ???
  def rollback(): Unit = ???
  def recover(): Unit = ???
  def pin(blk: BlockId): Unit = ???
  def unpin(blk: BlockId): Unit = ???
  def getInt(blk: BlockId, offset: Int): Int = ???
  def getString(blk: BlockId, offset: Int): String = ???
  def setInt(blk: BlockId, offset: Int, value: Int, okToLog: Boolean): Unit =
    ???
  def setString(
      blk: BlockId,
      offset: Int,
      value: String,
      okToLog: Boolean
  ): Unit = ???
  def availableBuffs: Int = ???
  def size(filename: String): Int = ???
  def append(filename: String): Block = ???
  def blockSize: Int = ???
}

// TODO
class Block
