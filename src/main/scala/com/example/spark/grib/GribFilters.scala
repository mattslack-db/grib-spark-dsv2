package com.example.spark.grib

import org.apache.spark.sql.sources._

/**
 * Single home for the connector's filter semantics, so the three sites that
 * reason about filters stay in lock-step:
 *
 *   1. [[GribScanBuilder]] — decides which filters to push down (`isSupported`).
 *   2. [[GribPartitionReader]] — the authoritative, exact evaluation after
 *      decoding (`passes`).
 *   3. [[GribScan]] — best-effort, superset pruning from `.idx` metadata
 *      before decoding (`indexEntryKept`).
 *
 * The invariant: a filter may be pushed (removed from Spark's post-scan
 * evaluation) only if `isSupported` returns true, and `passes` MUST fully and
 * exactly evaluate every such filter — otherwise Spark would drop it and the
 * predicate would be silently ignored. `isSupported` and `passes` therefore
 * cover exactly the same set of filter shapes.
 *
 * `indexEntryKept` is a pure optimization and is deliberately NOT the same as
 * `passes`: it may only prune on columns whose `.idx` vocabulary matches the
 * decoded output, and it must never be stricter than `passes` (or it would drop
 * rows that should survive). Today that is `variable_short_name` only, matched
 * case-insensitively; every other filter is kept for `passes` to decide.
 */
object GribFilters {

  /**
   * Whether a filter can be fully evaluated by [[passes]] (and is therefore
   * safe to push down). Keep this in exact correspondence with `passes`.
   */
  def isSupported(filter: Filter): Boolean = filter match {
    case EqualTo("variable_short_name", _: String)    => true
    case In("variable_short_name", _)                 => true
    case EqualTo("level_type", _: String)             => true
    case EqualTo("discipline", _: Int)                => true
    case GreaterThan("level_value", _: Number)        => true
    case GreaterThanOrEqual("level_value", _: Number) => true
    case LessThan("level_value", _: Number)           => true
    case LessThanOrEqual("level_value", _: Number)    => true
    case And(left, right)                             => isSupported(left) && isSupported(right)
    case Or(left, right)                              => isSupported(left) && isSupported(right)
    case Not(child)                                   => isSupported(child)
    case _                                            => false
  }

  /**
   * Authoritative, exact evaluation of a pushed filter against a decoded
   * message. Matching honours SQL semantics: equality is exact (case-sensitive).
   *
   * The `case _ => true` fall-through is only ever reached by filters that
   * `isSupported` rejects (which are never pushed, so Spark evaluates them
   * itself) — it must never be relied upon to filter.
   */
  def passes(msg: DecodedGribMessage, filter: Filter): Boolean = filter match {
    case EqualTo("variable_short_name", v: String)    => msg.variableShortName == v
    case In("variable_short_name", values)            => values.map(_.toString).contains(msg.variableShortName)
    case EqualTo("level_type", v: String)             => msg.levelType == v
    case EqualTo("discipline", v: Int)                => msg.discipline == v
    case GreaterThan("level_value", v: Number)        => msg.levelValue > v.doubleValue()
    case GreaterThanOrEqual("level_value", v: Number) => msg.levelValue >= v.doubleValue()
    case LessThan("level_value", v: Number)           => msg.levelValue < v.doubleValue()
    case LessThanOrEqual("level_value", v: Number)    => msg.levelValue <= v.doubleValue()
    case And(left, right)                             => passes(msg, left) && passes(msg, right)
    case Or(left, right)                              => passes(msg, left) || passes(msg, right)
    case Not(child)                                   => !passes(msg, child)
    case _                                            => true
  }

  /**
   * Best-effort, superset pruning of an `.idx` entry for a pushed filter.
   *
   * Only prunes on `variable_short_name` (case-insensitive), because that token
   * is shared between the `.idx` and the decoded output (both the NCEP short
   * name). It intentionally does NOT recurse into And/Or/Not or touch other
   * columns: keeping such entries is conservative and safe, since the
   * authoritative [[passes]] runs after decoding. It must never drop an entry
   * that `passes` would keep.
   */
  def indexEntryKept(entry: GribIndexEntry, filter: Filter): Boolean = filter match {
    case EqualTo("variable_short_name", v: String) => entry.variableName.equalsIgnoreCase(v)
    case In("variable_short_name", values)         => values.exists(x => entry.variableName.equalsIgnoreCase(x.toString))
    case _                                         => true
  }

  /**
   * Case-insensitive match for the `variables` option (a user-convenience
   * filter, applied consistently at both the index and message level). An empty
   * set means "keep everything".
   */
  def matchesAnyVariable(name: String, variables: Set[String]): Boolean =
    variables.isEmpty || variables.exists(_.equalsIgnoreCase(name))
}
