package com.github.viyadb.spark.saving

import org.apache.spark.sql.DataFrame

abstract class Saver extends Serializable {

  /**
    * Save data frame under given path
    *
    * @param df   Data frame
    * @param path Absolute path to directory under which files will be saved
    */
  def save(df: DataFrame, path: String)
}
