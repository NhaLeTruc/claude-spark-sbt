package com.etl.model

/**
 * Result of a load operation.
 *
 * @param recordsLoaded Number of records successfully written
 * @param recordsFailed Number of records that failed to write
 * @param errors List of error messages if any failures occurred
 */
case class LoadResult(
  recordsLoaded: Long,
  recordsFailed: Long = 0L,
  errors: Seq[String] = Seq.empty
) {
  def isSuccess: Boolean = recordsFailed == 0 && errors.isEmpty
}

object LoadResult {
  def success(recordCount: Long): LoadResult =
    LoadResult(recordsLoaded = recordCount)

  def failure(recordCount: Long, failedCount: Long, error: String): LoadResult =
    LoadResult(recordsLoaded = recordCount, recordsFailed = failedCount, errors = Seq(error))
}
