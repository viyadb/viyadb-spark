package com.github.viyadb.spark.streaming

class PathTracker(pathAccumulator: PathAccumulator, prefix: String) extends Serializable {

  private val seenPaths = scala.collection.mutable.Set[String]()

  def trackPath(path: String) = {
    val fullPath = s"${prefix}/${path}"
    // Only invoke accumulator if locally we are seing a new value:
    if (seenPaths.add(fullPath)) {
      pathAccumulator.add(fullPath)
    }
  }
}
