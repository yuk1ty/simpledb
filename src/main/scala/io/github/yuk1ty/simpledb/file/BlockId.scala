package io.github.yuk1ty.simpledb.file

case class BlockId(val fileName: String, val blknum: Int) {
  override def toString(): String = s"[file ${fileName}, block ${blknum}]"
}
