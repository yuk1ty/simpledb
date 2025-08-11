package io.github.yuk1ty.simpledb.file

import java.io.File
import java.io.RandomAccessFile
import scala.concurrent.SyncVar
import java.io.IOException
import scala.collection.mutable.HashMap
import scala.util.Try

object FileMgr {
  def apply(dbDirectory: File, blockSize: Int): FileMgr = {
    val isNew = !dbDirectory.exists()
    if (!isNew) {
      dbDirectory.mkdirs()
    }
    dbDirectory.list().foreach { filename =>
      if (filename.startsWith("temp")) {
        new File(dbDirectory, filename).delete()
      }
    }
    new FileMgr(dbDirectory, blockSize, isNew, HashMap())
  }
}

class FileMgr(
    private val dbDirectory: File,
    val blockSize: Int,
    val isNew: Boolean,
    private val openFiles: HashMap[String, RandomAccessFile]
) {
  def read(blk: BlockId, p: Page): Either[RuntimeException, Unit] = {
    synchronized {
      for {
        // TODO: RuntimeException!?
        f <- getFile(blk.fileName).left
          .map(_ => RuntimeException(s"cannot read block $blk"))
      } yield {
        f.seek(blk.blknum * blockSize)
        f.getChannel().read(p.contents())
      }
    }
  }

  def write(blk: BlockId, p: Page): Either[RuntimeException, Unit] = {
    synchronized {
      for {
        // TODO: RuntimeException!?
        f <- getFile(blk.fileName).left.map(_ =>
          RuntimeException(s"cannot write block $blk")
        )
      } yield {
        f.seek(blk.blknum * blockSize)
        f.getChannel().write(p.contents())
      }
    }
  }

  def append(fileName: String): Either[RuntimeException, Unit] = {
    for {
      // TODO: RuntimeException!?
      newBlkNum <- length(fileName)
      blk = BlockId(fileName, newBlkNum)
      b = new Array[Byte](blockSize)
      f <- getFile(blk.fileName).left.map(_ =>
        RuntimeException(s"cannot append block $blk")
      )
    } yield {
      f.seek(blk.blknum * blockSize)
      f.write(b)
    }
  }

  def length(fileName: String): Either[RuntimeException, Int] = {
    for {
      f <- getFile(fileName).left.map(_ =>
        RuntimeException(s"cannot access $fileName")
      )
    } yield (f.length() / blockSize).toInt
  }

  private def getFile(
      fileName: String
  ): Either[IOException, RandomAccessFile] = {
    openFiles.get(fileName) match
      case None => {
        for {
          dbTable <- Right(File(dbDirectory, fileName))
          random <- {
            try { Right(RandomAccessFile(dbTable, "rws")) }
            catch { case e: IOException => Left(e) }
          }
          _ = openFiles.put(fileName, random)
        } yield {
          random
        }
      }
      case Some(f) => Right(f)
  }
}
