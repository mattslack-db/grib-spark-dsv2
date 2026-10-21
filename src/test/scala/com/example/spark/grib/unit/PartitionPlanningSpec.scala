package com.example.spark.grib.unit

import com.example.spark.grib.{MessageRange, PartitionPlanning}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class PartitionPlanningSpec extends AnyFunSuite with Matchers {

  private val MB = 1024L * 1024L

  test("coalesce merges ranges that are adjacent in the file into one read") {
    val ranges = Seq(MessageRange(0, 0L, 100L), MessageRange(1, 100L, 50L), MessageRange(2, 150L, 25L))

    PartitionPlanning.coalesce(ranges) shouldBe Seq(MessageRange(0, 0L, 175L))
  }

  test("coalesce keeps a gap left by a filtered-out message, so it is never read") {
    // Message 1 (bytes 100-149) was filtered out.
    val ranges = Seq(MessageRange(0, 0L, 100L), MessageRange(2, 150L, 25L), MessageRange(3, 175L, 25L))

    PartitionPlanning.coalesce(ranges) shouldBe Seq(MessageRange(0, 0L, 100L), MessageRange(2, 150L, 50L))
  }

  test("coalesce handles empty and single-range input") {
    PartitionPlanning.coalesce(Seq.empty) shouldBe empty
    PartitionPlanning.coalesce(Seq(MessageRange(4, 10L, 5L))) shouldBe Seq(MessageRange(4, 10L, 5L))
  }

  test("pack groups ranges up to the target size, at least one per partition") {
    val ranges = (0 until 5).map(i => MessageRange(i, i * 40L, 40L))

    PartitionPlanning.pack(ranges, targetBytes = 100L).map(_.map(_.messageIndex)) shouldBe
      Seq(Seq(0, 1), Seq(2, 3), Seq(4))
    PartitionPlanning.pack(Seq(MessageRange(0, 0L, 500L)), targetBytes = 100L).map(_.size) shouldBe Seq(1)
    PartitionPlanning.pack(Seq.empty, targetBytes = 100L) shouldBe empty
  }

  test("target size gives each core a few partitions, within bounds") {
    // 218 MB over 8 cores, 3 partitions per core -> ~9 MB per partition, so a
    // straggling or oddly sized partition costs at most about a third of a round.
    val tasks = 8 * PartitionPlanning.PartitionsPerCore
    PartitionPlanning.targetPartitionBytes(218 * MB, parallelism = 8, maxPartitionBytes = 128 * MB) shouldBe
      (218 * MB + tasks - 1) / tasks
    // Lots of data: capped at maxPartitionBytes.
    PartitionPlanning.targetPartitionBytes(100000 * MB, parallelism = 8, maxPartitionBytes = 128 * MB) shouldBe 128 * MB
    // Very little data: no smaller than the minimum, to avoid a partition per tiny message.
    PartitionPlanning.targetPartitionBytes(1 * MB, parallelism = 8, maxPartitionBytes = 128 * MB) shouldBe
      PartitionPlanning.MinPartitionBytes
  }

  test("pack stays linear in the number of ranges") {
    // ~200k small fields into 128 MiB partitions: thousands of ranges per partition.
    val ranges = (0 until 200000).map(i => MessageRange(i, i * 60000L, 60000L))
    val start = System.nanoTime()
    val groups = PartitionPlanning.pack(ranges, targetBytes = 128 * MB)
    val seconds = (System.nanoTime() - start) / 1e9

    groups.map(_.size).sum shouldBe ranges.size
    groups.foreach(g => g.map(_.length).sum should be <= 128 * MB)
    seconds should be < 1.0
  }
}

