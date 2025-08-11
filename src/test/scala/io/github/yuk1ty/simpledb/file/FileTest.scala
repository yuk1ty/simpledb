package io.github.yuk1ty.simpledb.file

import io.github.yuk1ty.simpledb.file.BlockId
import io.github.yuk1ty.simpledb.file.Page
import io.github.yuk1ty.simpledb.file.FileMgr
import java.io.File

class FileTest extends munit.FunSuite {
  test("read") {
    // Given
    val fm = FileMgr(File("./data"), 1024 * 8)
    val blk = BlockId("testfile", 2)
    val p1 = Page(fm.blockSize)
    val pos1 = 88
    val givenString = "abcdefghijklm"
    p1.setString(pos1, givenString)
    val size = Page.maxLength(givenString.length())
    val pos2 = pos1 + size
    p1.setInt(pos2, 345)
    fm.write(blk, p1)

    // When
    val p2 = Page(fm.blockSize)
    fm.read(blk, p2)

    // Then
    assertEquals(p2.getInt(pos2), 345)
    assertEquals(p2.getString(pos1), "abcdefghijklm")
  }
}
