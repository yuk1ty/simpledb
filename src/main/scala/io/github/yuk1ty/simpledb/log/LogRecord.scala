package io.github.yuk1ty.simpledb.log

import io.github.yuk1ty.simpledb.buffer.TxNum
import io.github.yuk1ty.simpledb.file.Page

trait LogRecord {
  def op: Int
  def txNumber: TxNum
  def undo(txnum: TxNum): Unit
}

object LogRecord {
  def createLogRecord(
      bytes: Array[Byte]
  ): Either[RuntimeException, LogRecord] = {
    val p = Page(bytes)
    p.getInt(0) match {
      case Op.Checkpoint.opcode => ???
      case Op.Start.opcode      => ???
      case Op.Commit.opcode     => ???
      case Op.Rollback.opcode   => ???
      case Op.SetInt.opcode     => ???
      case Op.SetString.opcode  => ???
      case _                    => Left(RuntimeException("unknown op"))
    }
  }
}

enum Op(val opcode: Int) {
  case Checkpoint extends Op(0)
  case Start extends Op(1)
  case Commit extends Op(2)
  case Rollback extends Op(3)
  case SetInt extends Op(4)
  case SetString extends Op(5)
}
