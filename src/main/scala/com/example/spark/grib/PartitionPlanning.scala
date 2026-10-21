package com.example.spark.grib

/**
 * Pure helpers for splitting `.idx`-located messages into Spark partitions and
 * into reads.
 */
object PartitionPlanning {

  /**
   * Smallest byte-based partition, so many tiny messages (e.g. a 1° GFS file's
   * ~60 KB fields) aren't spread one or two per task. Plays the role of Spark's
   * spark.sql.files.openCostInBytes.
   */
  val MinPartitionBytes: Long = 4L * 1024 * 1024

  /**
   * Partitions to aim for per core. With exactly one, partition sizes that
   * don't divide evenly (or one slow partition) leave cores idle for up to a
   * whole round; with three, at most about a third of one.
   */
  val PartitionsPerCore = 3

  /**
   * Bytes per partition: spread the data evenly over [[PartitionsPerCore]]
   * partitions per core, but no partition larger than `maxPartitionBytes` or
   * smaller than [[MinPartitionBytes]].
   */
  def targetPartitionBytes(totalBytes: Long, parallelism: Int, maxPartitionBytes: Long): Long = {
    val partitions = math.max(parallelism, 1).toLong * PartitionsPerCore
    val perPartition = (totalBytes + partitions - 1) / partitions
    math.min(maxPartitionBytes, math.max(MinPartitionBytes, perPartition))
  }

  /**
   * Group a file's message ranges (in file order) into partitions of up to
   * `targetBytes`. A message larger than the target gets a partition of its own.
   */
  def pack(ranges: Seq[MessageRange], targetBytes: Long): Seq[Seq[MessageRange]] = {
    // Fold state: finished groups, the group being filled, and its byte total.
    val (done, current, _) = ranges.foldLeft((Vector.empty[Vector[MessageRange]], Vector.empty[MessageRange], 0L)) {
      case ((groups, group, bytes), range) if group.nonEmpty && bytes + range.length > targetBytes =>
        (groups :+ group, Vector(range), range.length)
      case ((groups, group, bytes), range) =>
        (groups, group :+ range, bytes + range.length)
    }
    if (current.isEmpty) done else done :+ current
  }

  /**
   * Merge ranges that are adjacent in the file into single reads, so a
   * partition of consecutive messages is fetched with one sequential read
   * rather than one request per message. Gaps (messages filtered out) are kept,
   * so they are never read. A merged range takes its first message's index;
   * decoding it numbers the following fields in order, which matches their own
   * indices because nothing between them was skipped.
   */
  def coalesce(ranges: Seq[MessageRange]): Seq[MessageRange] =
    ranges.foldLeft(Vector.empty[MessageRange]) { (merged, range) =>
      merged.lastOption match {
        case Some(last) if last.offset + last.length == range.offset =>
          merged.init :+ last.copy(length = last.length + range.length)
        case _ =>
          merged :+ range
      }
    }
}
