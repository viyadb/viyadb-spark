package com.github.viyadb.spark.notifications

import com.github.viyadb.spark.Configs.NotifierConf
import com.github.viyadb.spark.util.FileSystemUtil

class FileNotifier[A <: AnyRef](notifierConf: NotifierConf)(implicit m: Manifest[A]) extends Notifier[A] {

  override def send(batchId: Long, info: A) = {
    FileSystemUtil.setContent(s"${notifierConf.channel}/${notifierConf.queue}/${batchId}.msg", writeMessage(info))
  }

  override def lastMessage = {
    val prefix = s"${notifierConf.channel}/${notifierConf.queue}"
    FileSystemUtil.list(prefix).map(_.getPath.getName).filter(_.endsWith(".msg"))
      .sorted.lastOption.map { lastFile =>
      readMessage(FileSystemUtil.getContent(s"$prefix/$lastFile"))
    }
  }

  override def allMessages = {
    val prefix = s"${notifierConf.channel}/${notifierConf.queue}"
    FileSystemUtil.list(prefix).map(_.getPath.getName).filter(_.endsWith(".msg"))
      .sorted.map { file =>
      readMessage(FileSystemUtil.getContent(s"$prefix/$file"))
    }
  }
}
