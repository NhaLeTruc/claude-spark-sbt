package com.etl.model

/**
 * Write mode enumeration for data loading operations.
 * Defines how data should be written to the target sink.
 */
sealed trait WriteMode

object WriteMode {
  /** Append new records to existing data */
  case object Append extends WriteMode

  /** Replace all existing data with new records */
  case object Overwrite extends WriteMode

  /** Insert new records and update existing records based on primary keys */
  case object Upsert extends WriteMode

  /**
   * Parse write mode from string (case-insensitive).
   *
   * @param mode String representation of write mode
   * @return WriteMode instance
   * @throws IllegalArgumentException if mode is not recognized
   */
  def fromString(mode: String): WriteMode = mode.toLowerCase match {
    case "append"    => Append
    case "overwrite" => Overwrite
    case "upsert"    => Upsert
    case _           => throw new IllegalArgumentException(s"Unknown write mode: $mode")
  }
}
