package com.example.spark.grib

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.read.{Batch, InputPartition, PartitionReaderFactory, Scan}
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.StructType
import org.apache.hadoop.fs.{FileSystem, Path}
import org.slf4j.LoggerFactory
import java.io.{BufferedReader, InputStreamReader}
import java.net.URI
import java.util.concurrent.Executors
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration.Duration

/**
 * Plans the input partitions for reading GRIB files. Uses .idx sidecar files
 * when available for sub-file partitioning; falls back to whole-file partitions.
 */
class GribScan(
    options: GribOptions,
    readSchema: StructType,
    pushedFilters: Array[Filter]
) extends Scan with Batch {

  private val logger = LoggerFactory.getLogger(getClass)

  // Capture the session's Hadoop configuration on the driver (includes
  // spark.hadoop.* and cloud credentials) and ship it to executors, rather than
  // building a bare `new Configuration()` there that would miss those settings.
  private val serializableConf =
    new SerializableHadoopConf(SparkSession.active.sessionState.newHadoopConf())

  override def readSchema(): StructType = readSchema

  override def toBatch: Batch = this

  override def planInputPartitions(): Array[InputPartition] = {
    val hadoopConf = serializableConf.value

    // Pass 1: find each file and, where it has a usable .idx, the byte ranges
    // of the messages that survive index pruning. That costs a few storage round
    // trips per file (the .idx, and the GRIB header for level pruning), so files
    // are planned concurrently.
    val files = options.paths.flatMap { inputPath =>
      val path = new Path(inputPath)
      val fs = FileSystem.get(path.toUri, hadoopConf)
      listGribFiles(fs, path, inputPath).map(fs -> _)
    }
    val plans = GribScan.inParallel(files, GribScan.PlanningThreads) { case (fs, file) => planFile(fs, file) }

    // Pass 2: split into partitions. Without a fixed messagesPerPartition, size
    // them by bytes so the surviving data spreads evenly across the cluster.
    val targetBytes = PartitionPlanning.targetPartitionBytes(
      plans.map(_.bytes).sum, SparkSession.active.sparkContext.defaultParallelism, options.maxPartitionBytes)

    val partitions = plans.flatMap {
      case FilePlan(filePath, None, _) =>
        Seq(GribInputPartition(filePath, None, pushedFilters, options.outputMode))
      case FilePlan(filePath, Some(ranges), _) =>
        val groups = options.messagesPerPartition match {
          case Some(n) => ranges.grouped(n).toSeq
          case None    => PartitionPlanning.pack(ranges, targetBytes)
        }
        groups.map(g => GribInputPartition(filePath, Some(g), pushedFilters, options.outputMode))
    }

    logger.info(s"Planned ${partitions.size} partition(s) across ${plans.size} file(s), " +
      s"target ${targetBytes / 1024} KiB per partition")
    partitions.toArray
  }

  private def planFile(fs: FileSystem, file: GribFile): FilePlan = {
    val filePath = file.path.toString
    val idxPath = new Path(filePath + ".idx")
    val hasIdx = options.useIndex && file.hasIdx.getOrElse(fs.exists(idxPath))
    val ranges = if (hasIdx) keptMessageRanges(fs, filePath, idxPath, file.length) else None
    FilePlan(filePath, ranges, ranges.fold(file.length)(_.map(_.length).sum))
  }

  /**
   * Expand a glob or directory, skipping sidecars and other non-GRIB files. A
   * path naming a single file is always read. Sizes come from the listing, and
   * so does `.idx` presence for a directory, which lists every sidecar (a glob
   * may not match them, so that's left to check per file).
   */
  private def listGribFiles(fs: FileSystem, path: Path, inputPath: String): Seq[GribFile] = {
    val isGlob = inputPath.contains("*") || inputPath.contains("?") || inputPath.contains("[")
    if (isGlob || fs.isDirectory(path)) {
      val statuses = if (isGlob) Option(fs.globStatus(path)).getOrElse(Array.empty) else fs.listStatus(path)
      val listed = statuses.map(_.getPath.toString).toSet
      statuses.toSeq
        .filter(s => !s.isDirectory && GribScan.isGribFileCandidate(s.getPath.getName))
        .map { s =>
          val idxListed = listed.contains(s.getPath.toString + ".idx")
          GribFile(s.getPath, s.getLen, if (idxListed || !isGlob) Some(idxListed) else None)
        }
    } else {
      // Keep the path as given: an unqualified local or FUSE path (/Volumes/...) is read in place.
      Seq(GribFile(path, fs.getFileStatus(path).getLen, hasIdx = None))
    }
  }

  override def createReaderFactory(): PartitionReaderFactory = {
    new GribPartitionReaderFactory(options, serializableConf)
  }

  /**
   * Byte ranges, in file order, of the messages an `.idx` sidecar locates that
   * survive index pruning, or None if the sidecar is malformed (the file is then
   * read whole).
   */
  private def keptMessageRanges(
      fs: FileSystem,
      filePath: String,
      idxPath: Path,
      fileSize: Long
  ): Option[Seq[MessageRange]] = {
    val parsed = GribIndexParser.parseStrict(readTextFile(fs, idxPath))
    if (parsed.isEmpty) logger.warn(s"Ignoring malformed index $idxPath; reading $filePath whole")
    parsed.map(allEntries => keptRanges(fs, filePath, allEntries, fileSize))
  }

  private def keptRanges(
      fs: FileSystem,
      filePath: String,
      allEntries: Seq[GribIndexEntry],
      fileSize: Long
  ): Seq[MessageRange] = {

    // Group into physical GRIB messages and compute byte ranges on the FULL,
    // in-file-order entry list. This MUST happen before any filtering —
    // dropping entries first would make a surviving message's range span the
    // filtered-out messages that physically follow it (and the last survivor
    // would extend to EOF), re-decoding data that was meant to be skipped. Each
    // message's message_index (the position of its first field in the full
    // list) is fixed here for the same reason. A message packing several fields
    // (sub-messages) is read once if any of its fields is wanted.
    // Level pruning maps .idx level text to GRIB2 level names, so it needs the
    // file's edition: one small read, made only when a level filter is pushed.
    val levelsComparable = hasLevelFilter && readEdition(fs, filePath) == GribEdition2
    val levelOf: GribIndexEntry => Option[IndexLevel] =
      if (levelsComparable) e => GribIndexLevels.parse(e.levelDescription) else _ => None

    val kept = GribIndexParser.groupMessages(allEntries, fileSize)
      .filter(m => m.entries.exists(e => isIndexEntryKept(e, levelOf(e))))
      .map(_.range)

    if (kept.isEmpty) logger.info(s"All messages filtered out for $filePath")
    kept
  }

  /**
   * Best-effort pruning of `.idx` entries before decoding, to skip byte ranges
   * that cannot match. This is NOT the authoritative filter — the reader
   * re-evaluates every filter exactly after decoding (see
   * [[GribFilters.passes]]) — so it must never drop an entry the message-level
   * filter would keep.
   *
   * It prunes on `variable_short_name`, whose `.idx` token is the emitted value
   * up to case for all but a few known parameters (see
   * [[GribFilters.idxNameMayBe]]), and on level only
   * where [[GribIndexLevels]] maps the `.idx` level text ("500 mb") to the
   * decoded `level_type`/`level_value`, in GRIB2 files. Unrecognised levels are
   * never pruned; they're filtered after decoding.
   */
  private def isIndexEntryKept(entry: GribIndexEntry, level: Option[IndexLevel]): Boolean =
    // Option-level variable filter (GribFilters.indexEntryMatchesAnyVariable,
    // never stricter than matchesAnyVariable on the decoded message), then pushed SQL
    // filters — superset pruning on variable_short_name, and on level when known.
    GribFilters.indexEntryMatchesAnyVariable(entry.variableName, options.variables) &&
      pushedFilters.forall(GribFilters.indexEntryKept(entry, _, level))

  private val GribEdition2 = 2

  private lazy val hasLevelFilter: Boolean =
    pushedFilters.exists(_.references.exists(c => c == "level_value" || c == "level_type"))

  /** The GRIB edition from the first message's header, or 0 if unreadable. */
  private def readEdition(fs: FileSystem, filePath: String): Int = {
    val in = fs.open(new Path(filePath))
    try {
      val header = new Array[Byte](GribIo.HeaderBytes)
      in.readFully(0L, header)
      GribIo.edition(header)
    } catch {
      case _: java.io.IOException => 0
    } finally {
      in.close()
    }
  }

  /**
   * Read a text file from any Hadoop-compatible filesystem.
   */
  private def readTextFile(fs: FileSystem, path: Path): String = {
    val stream = fs.open(path)
    try {
      val reader = new BufferedReader(new InputStreamReader(stream, "UTF-8"))
      val sb = new StringBuilder
      var line = reader.readLine()
      while (line != null) {
        sb.append(line).append('\n')
        line = reader.readLine()
      }
      sb.toString()
    } finally {
      stream.close()
    }
  }
}

/** A GRIB file found by listing. `hasIdx` is None when the listing can't tell whether it has a sidecar. */
private case class GribFile(path: Path, length: Long, hasIdx: Option[Boolean])

/** A file to read: whole, or as the given `.idx`-located message ranges. `bytes` is how much will be read. */
private case class FilePlan(filePath: String, ranges: Option[Seq[MessageRange]], bytes: Long)

object GribScan {

  /** Files planned concurrently on the driver; planning a file is a few storage round trips. */
  private[grib] val PlanningThreads = 16

  /** `items.map(f)` on up to `threads` threads, in order. The first failure is rethrown. */
  private[grib] def inParallel[A, B](items: Seq[A], threads: Int)(f: A => B): Seq[B] =
    if (items.size <= 1) items.map(f)
    else {
      val pool = Executors.newFixedThreadPool(math.min(threads, items.size))
      implicit val ec: ExecutionContext = ExecutionContext.fromExecutorService(pool)
      try Await.result(Future.traverse(items)(item => Future(f(item))), Duration.Inf)
      finally pool.shutdown()
    }

  // Extensions of files that sit alongside GRIB data but aren't GRIB (or can't
  // be read as GRIB as they are, like compressed archives).
  private val NonGribExtensions = Set(
    "idx", "md5", "sha1", "sha256", "sha512", "crc",            // sidecars and checksums
    "gbx9", "ncx", "ncx2", "ncx3", "ncx4", "ncml",              // netCDF-Java indexes and catalogs
    "part", "tmp",                                              // partial downloads
    "txt", "md", "json", "xml", "html", "log", "pdf",           // documents
    "csv", "tsv", "yaml", "yml",                                // tables and config
    "nc", "nc4", "h5", "hdf", "parquet",                        // other data formats
    "gz", "zip", "tar", "bz2", "xz", "zst",                     // archives
    "png", "jpg", "jpeg", "gif",                                // images
    "py", "sh"                                                  // scripts
  )

  /**
   * Whether a file found by listing a directory or expanding a glob should be
   * read as GRIB. GRIB files have no reliable extension (NOAA names such as
   * `gfs.t00z.pgrb2.1p00.f000` use dots but no `.grib2`), so rather than
   * allow-listing extensions this skips known non-GRIB files: the above, plus
   * hidden files and Spark/Hadoop markers (`.name`, `_SUCCESS`).
   */
  def isGribFileCandidate(fileName: String): Boolean = {
    val extension = fileName.lastIndexOf('.') match {
      case -1 => ""
      case i  => fileName.substring(i + 1).toLowerCase
    }
    !fileName.startsWith(".") && !fileName.startsWith("_") && !NonGribExtensions(extension)
  }
}
