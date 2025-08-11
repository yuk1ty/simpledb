package io.github.yuk1ty.simpledb.file

import java.nio.ByteBuffer
import java.nio.charset.Charset
import java.nio.charset.StandardCharsets
import io.github.yuk1ty.simpledb.file.Page.CHARSET

object Page {
  val CHARSET: Charset = StandardCharsets.US_ASCII

  inline def apply(blockSize: Int): Page = new Page(
    ByteBuffer.allocateDirect(blockSize)
  )

  inline def apply(b: Array[Byte]): Page = new Page(ByteBuffer.wrap(b))

  inline def maxLength(strlen: Int): Int = {
    val bytesPerChar = CHARSET.newEncoder().maxBytesPerChar()
    Integer.BYTES + (strlen * bytesPerChar.toInt)
  }
}

class Page(private var bb: ByteBuffer) {
  def getInt(offset: Int): Int = bb.getInt(offset)

  def setInt(offset: Int, n: Int): Unit = bb.putInt(offset, n)

  def getBytes(offset: Int): Array[Byte] = {
    bb.position(offset)
    val length = bb.getInt()
    val b = new Array[Byte](length)
    bb.get(b)
    b
  }

  def setBytes(offset: Int, b: Array[Byte]): Unit = {
    bb.position(offset)
    bb.putInt(b.length)
    bb.put(b)
  }

  def getString(offset: Int): String = {
    val b = getBytes(offset)
    String(b, CHARSET)
  }

  def setString(offset: Int, s: String): Unit = {
    val b = s.getBytes(CHARSET)
    setBytes(offset, b)
  }

  private[file] def contents(): ByteBuffer = {
    bb.position(0)
    bb
  }
}
