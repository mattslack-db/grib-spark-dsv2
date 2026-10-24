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
 * `passes`: it may only prune on what an `.idx` line reliably says, and it must
 * never be stricter than `passes` (or it would drop rows that should survive).
 * That is `variable_short_name` (matched case-insensitively, allowing for the
 * few parameters whose `.idx` name differs from the decoded one, see
 * [[idxNameMayBe]]) and, when the line's level text maps to a known decoded
 * level ([[GribIndexLevels]]), `level_type`/`level_value`. Everything else is
 * unknown and kept for `passes`.
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
    case EqualTo("level_value", _: Number)            => true
    case In("level_value", values)                    => values.forall(_.isInstanceOf[Number])
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
    case EqualTo("level_value", v: Number)            => sqlCompare(msg.levelValue, v) == 0
    case In("level_value", values)                    => values.exists(v => sqlCompare(msg.levelValue, v.asInstanceOf[Number]) == 0)
    case GreaterThan("level_value", v: Number)        => sqlCompare(msg.levelValue, v) > 0
    case GreaterThanOrEqual("level_value", v: Number) => sqlCompare(msg.levelValue, v) >= 0
    case LessThan("level_value", v: Number)           => sqlCompare(msg.levelValue, v) < 0
    case LessThanOrEqual("level_value", v: Number)    => sqlCompare(msg.levelValue, v) <= 0
    case And(left, right)                             => passes(msg, left) && passes(msg, right)
    case Or(left, right)                              => passes(msg, left) || passes(msg, right)
    case Not(child)                                   => !passes(msg, child)
    case _                                            => true
  }

  /**
   * Compare doubles as Spark SQL does, since Spark doesn't re-check pushed
   * filters: NaN equals NaN and is larger than any other value, and -0.0 equals
   * 0.0 (unlike Scala's `==`/`<` on doubles).
   */
  private def sqlCompare(a: Double, b: Number): Int = {
    val y = b.doubleValue()
    if (a.isNaN || y.isNaN) java.lang.Boolean.compare(a.isNaN, y.isNaN)
    else java.lang.Double.compare(a + 0.0, y + 0.0) // + 0.0 turns -0.0 into 0.0
  }

  /**
   * Best-effort, superset pruning of an `.idx` entry for a pushed filter: false
   * only if the entry's message definitely fails it. `level` is the entry's
   * level as the decoder would report it, when [[GribIndexLevels]] recognises
   * the `.idx` text (and the file is GRIB2); None leaves level filters unknown.
   * It must never drop an entry that `passes` would keep.
   */
  def indexEntryKept(entry: GribIndexEntry, filter: Filter, level: Option[IndexLevel] = None): Boolean =
    indexVerdict(entry, level, filter) != Some(false)

  /**
   * Three-valued evaluation against `.idx` metadata: Some(true)/Some(false) when
   * the outcome is certain, None when the `.idx` line can't tell.
   */
  private def indexVerdict(entry: GribIndexEntry, level: Option[IndexLevel], filter: Filter): Option[Boolean] =
    filter match {
      // A match isn't certain (decoded matching is exact and case-sensitive),
      // but a name the .idx entry can't decode to is a certain mismatch.
      case EqualTo("variable_short_name", v: String) => falseUnless(idxNameMayBe(entry.variableName, v))
      case In("variable_short_name", values)         => falseUnless(values.exists(x => idxNameMayBe(entry.variableName, x.toString)))

      case EqualTo("level_type", v: String)             => level.map(_.levelType == v)
      case EqualTo("level_value", v: Number)            => level.flatMap(l => compare(l.value, v)(_ == 0))
      case In("level_value", values)                    => level.flatMap(l => anyOf(values.map(v => compare(l.value, v.asInstanceOf[Number])(_ == 0))))
      case GreaterThan("level_value", v: Number)        => level.flatMap(l => compare(l.value, v)(_ > 0))
      case GreaterThanOrEqual("level_value", v: Number) => level.flatMap(l => compare(l.value, v)(_ >= 0))
      case LessThan("level_value", v: Number)           => level.flatMap(l => compare(l.value, v)(_ < 0))
      case LessThanOrEqual("level_value", v: Number)    => level.flatMap(l => compare(l.value, v)(_ <= 0))

      case And(left, right) =>
        (indexVerdict(entry, level, left), indexVerdict(entry, level, right)) match {
          case (Some(false), _) | (_, Some(false)) => Some(false)
          case (Some(true), Some(true))            => Some(true)
          case _                                   => None
        }
      case Or(left, right) => anyOf(Seq(indexVerdict(entry, level, left), indexVerdict(entry, level, right)))
      case Not(child)      => indexVerdict(entry, level, child).map(!_)
      case _               => None
    }

  // Levels derived from .idx text (e.g. "0.4 mb" * 100) carry floating-point
  // error, so values this close to the filter's are treated as unknown.
  private val LevelTolerance = 1e-6

  /** Outcome of `cmp(sign of level - v)`, or None if the two are within tolerance (or either is NaN). */
  private def compare(levelValue: Double, v: Number)(cmp: Int => Boolean): Option[Boolean] = {
    val target = v.doubleValue()
    if (levelValue.isNaN || target.isNaN) return None
    val diff = levelValue - target
    if (math.abs(diff) <= LevelTolerance * math.max(1.0, math.abs(target))) None
    else Some(cmp(math.signum(diff).toInt))
  }

  private def falseUnless(maybeTrue: Boolean): Option[Boolean] = if (maybeTrue) None else Some(false)

  private def anyOf(verdicts: Seq[Option[Boolean]]): Option[Boolean] =
    if (verdicts.contains(Some(true))) Some(true)
    else if (verdicts.forall(_.contains(false))) Some(false)
    else None

  /**
   * wgrib2 `.idx` names that differ from netCDF-Java's decoded short names
   * beyond case, as observed in NOAA files (`.idx` name -> decoded name, upper
   * case). Without these, filtering on either name dropped the fields.
   */
  private val IdxNameAliases: Map[String, String] = Map(
    "CLMR" -> "CLWMR", // cloud mixing ratio
    "LFTX" -> "LFT X"  // surface lifted index
  )

  private val PlainAbbreviation = "^[A-Za-z0-9_]+$".r
  /** The decoder's name for a parameter its tables don't know (see GribMessageDecoder). */
  private val DecodedFallbackName = "^var\\d+_\\d+_\\d+$".r

  /**
   * Whether the field an `.idx` entry names `idxName` could decode with short
   * name `v`, ignoring case. Only plain abbreviations can be compared: wgrib2
   * writes a parameter without one as `var discipline=0 center=7 ... parm=195`,
   * which the decoder names `var0_16_195`, so such names never rule a field out.
   */
  def idxNameMayBe(idxName: String, v: String): Boolean =
    PlainAbbreviation.findFirstIn(idxName).isEmpty ||
      DecodedFallbackName.findFirstIn(v).isDefined ||
      idxName.equalsIgnoreCase(v) ||
      IdxNameAliases.get(idxName.toUpperCase).exists(_.equalsIgnoreCase(v))

  /**
   * The `variables` option (a user-convenience filter) against a decoded
   * short name: case-insensitive, and a field can also be requested by its
   * `.idx` name (e.g. `CLMR` for `CLWMR`). An empty set means "keep everything".
   */
  def matchesAnyVariable(name: String, variables: Set[String]): Boolean =
    variables.isEmpty || variables.exists { v =>
      v.equalsIgnoreCase(name) || IdxNameAliases.get(v.toUpperCase).exists(_.equalsIgnoreCase(name))
    }

  /**
   * The `variables` option against an `.idx` entry's name: kept if any
   * requested name could be its field's, by `.idx` or decoded name. Never
   * stricter than [[matchesAnyVariable]] on the decoded field.
   */
  def indexEntryMatchesAnyVariable(idxName: String, variables: Set[String]): Boolean =
    variables.isEmpty || variables.exists { v =>
      idxNameMayBe(idxName, v) || IdxNameAliases.get(v.toUpperCase).exists(idxNameMayBe(idxName, _))
    }
}
