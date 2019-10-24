/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.execution.datasources.oap.index.impl

import java.io.{File, FileInputStream, FileOutputStream, OutputStream}
import java.nio.channels.FileChannel

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataInputStream, Path}

import org.apache.spark.sql.execution.datasources.oap.index.IndexFileWriter
import org.apache.spark.sql.internal.oap.OapConf

private[index] case class IndexFileWriterImpl(
    configuration: Configuration,
    indexPath: Path) extends IndexFileWriter {

  protected override val os: OutputStream =
    indexPath.getFileSystem(configuration).create(indexPath, true)

  private val zeroCpEnable = configuration.getBoolean(
    OapConf.OAP_INDEX_FILE_WRITER_ZERO_COPY_ENABLE.key,
    OapConf.OAP_INDEX_FILE_WRITER_ZERO_COPY_ENABLE.defaultValue.get)

  private val outChannel = if (zeroCpEnable) {
    new FileOutputStream(new File(
      s"${indexPath.getParent}/${indexPath.getName}"), true).getChannel
  } else null

  // Give RecordWriter a chance which file it's writing to.
  override def getName: String = indexPath.toString

  override def tempRowIdWriter: IndexFileWriter = {
    val tempFileName = new Path(indexPath.getParent, indexPath.getName + ".id")
    IndexFileWriterImpl(configuration, tempFileName)
  }

  override def close(): Unit = {
    if (outChannel != null) {
      outChannel.close()
    }
    super.close()
  }

  override def writeRowId(tempWriter: IndexFileWriter): Unit = {
    val path = new Path(tempWriter.getName)
    val length: Long = path.getFileSystem(configuration).getFileStatus(path).getLen

    if (zeroCpEnable) {
      var inChannel: FileChannel = null
      try {
        inChannel = new FileInputStream(new File(s"${path.getParent}/${path.getName}")).getChannel
        var count: Long = 0
        while (count < length) {
          count += inChannel.transferTo(count, length - count, outChannel)
        }
      } finally {
        if (inChannel != null) {
          inChannel.close()
        }
        path.getFileSystem(configuration).delete(path, false)
      }
    } else {
      val bufSize = configuration.getInt("io.file.buffer.size", 4096)
      var is: FSDataInputStream = null
      val bytes = new Array[Byte](bufSize)
      try {
        is = path.getFileSystem(configuration).open(path)
        var remaining = length
        while (remaining > 0) {
          val readSize = math.min(bufSize, remaining).toInt
          is.readFully(bytes, 0, readSize)
          os.write(bytes, 0, readSize)
          remaining -= readSize
        }
      } finally {
        if (is != null) {
          is.close()
        }
        path.getFileSystem(configuration).delete(path, false)
      }
    }
  }
}
