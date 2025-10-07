package com.etl.util

import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

import scala.concurrent.{Await, Promise}
import scala.concurrent.duration._
import scala.util.{Failure, Success, Try}

/**
 * Graceful shutdown handler for ETL pipelines.
 * Handles SIGTERM and SIGINT signals to ensure clean shutdown of Spark applications.
 *
 * Features:
 * - Registers shutdown hooks for SIGTERM/SIGINT
 * - Stops SparkSession gracefully
 * - Waits for ongoing operations to complete (with timeout)
 * - Flushes logs and metrics
 * - Returns appropriate exit code
 *
 * Usage:
 * {{{
 * val shutdownHandler = new GracefulShutdown(spark, shutdownTimeoutSeconds = 60)
 * shutdownHandler.registerShutdownHook()
 *
 * // Your pipeline execution code here
 *
 * shutdownHandler.waitForShutdown() // Blocks until shutdown signal received
 * }}}
 */
class GracefulShutdown(
  spark: SparkSession,
  shutdownTimeoutSeconds: Int = 60
) {
  private val logger = LoggerFactory.getLogger(getClass)
  private val shutdownPromise = Promise[Unit]()
  private var shutdownInitiated = false

  /**
   * Register shutdown hook for SIGTERM and SIGINT.
   * This should be called early in application startup.
   */
  def registerShutdownHook(): Unit = {
    logger.info("Registering graceful shutdown hook")

    Runtime.getRuntime.addShutdownHook(new Thread() {
      override def run(): Unit = {
        performShutdown()
      }
    })

    logger.info("Shutdown hook registered successfully")
  }

  /**
   * Check if shutdown has been initiated.
   *
   * @return true if shutdown in progress
   */
  def isShutdownInitiated: Boolean = shutdownInitiated

  /**
   * Wait for shutdown signal (blocks indefinitely).
   * Used for streaming applications that run until interrupted.
   *
   * @return Unit when shutdown completes
   */
  def waitForShutdown(): Unit = {
    Try(Await.result(shutdownPromise.future, Duration.Inf)) match {
      case Success(_) =>
        logger.info("Shutdown completed successfully")

      case Failure(ex) =>
        logger.error(s"Error during shutdown wait: ${ex.getMessage}", ex)
    }
  }

  /**
   * Perform graceful shutdown of Spark and related resources.
   * Called automatically by shutdown hook or can be invoked manually.
   */
  def performShutdown(): Unit = {
    synchronized {
      if (shutdownInitiated) {
        logger.warn("Shutdown already in progress, ignoring duplicate request")
        return
      }
      shutdownInitiated = true
    }

    logger.info(s"Initiating graceful shutdown (timeout: ${shutdownTimeoutSeconds}s)")

    try {
      // Step 1: Stop accepting new work
      logger.info("Step 1/4: Stopping new work acceptance")
      // Application-specific logic here (e.g., stop consuming from Kafka)

      // Step 2: Wait for ongoing operations to complete
      logger.info("Step 2/4: Waiting for ongoing operations to complete")
      waitForOngoingOperations()

      // Step 3: Stop SparkSession
      logger.info("Step 3/4: Stopping SparkSession")
      stopSparkSession()

      // Step 4: Flush logs and metrics
      logger.info("Step 4/4: Flushing logs and metrics")
      flushLogsAndMetrics()

      logger.info("Graceful shutdown completed successfully")
      shutdownPromise.success(())

    } catch {
      case ex: Exception =>
        logger.error(s"Error during graceful shutdown: ${ex.getMessage}", ex)
        shutdownPromise.failure(ex)
    }
  }

  /**
   * Wait for ongoing Spark operations to complete.
   * Uses timeout to prevent indefinite blocking.
   */
  private def waitForOngoingOperations(): Unit = {
    val startTime = System.currentTimeMillis()
    val timeoutMillis = shutdownTimeoutSeconds * 1000L

    try {
      // Check active Spark jobs
      val activeJobIds = spark.sparkContext.statusTracker.getActiveJobIds()

      if (activeJobIds.nonEmpty) {
        logger.info(s"Waiting for ${activeJobIds.length} active job(s) to complete")

        while (spark.sparkContext.statusTracker.getActiveJobIds().nonEmpty) {
          val elapsed = System.currentTimeMillis() - startTime

          if (elapsed > timeoutMillis) {
            logger.warn(
              s"Timeout waiting for jobs to complete after ${shutdownTimeoutSeconds}s. " +
                "Proceeding with shutdown anyway."
            )
            return
          }

          Thread.sleep(1000)
          logger.debug(
            s"Still waiting for jobs... (elapsed: ${elapsed / 1000}s / ${shutdownTimeoutSeconds}s)"
          )
        }

        logger.info("All active jobs completed")
      } else {
        logger.info("No active jobs to wait for")
      }

    } catch {
      case ex: Exception =>
        logger.error(s"Error waiting for ongoing operations: ${ex.getMessage}", ex)
        // Continue with shutdown anyway
    }
  }

  /**
   * Stop SparkSession gracefully.
   */
  private def stopSparkSession(): Unit = {
    try {
      if (!spark.sparkContext.isStopped) {
        logger.info("Stopping SparkSession...")
        spark.stop()
        logger.info("SparkSession stopped successfully")
      } else {
        logger.info("SparkSession already stopped")
      }
    } catch {
      case ex: Exception =>
        logger.error(s"Error stopping SparkSession: ${ex.getMessage}", ex)
    }
  }

  /**
   * Flush logs and metrics before exit.
   */
  private def flushLogsAndMetrics(): Unit = {
    try {
      // Flush Logback appenders
      val loggerContext = LoggerFactory.getILoggerFactory
        .asInstanceOf[ch.qos.logback.classic.LoggerContext]

      logger.info("Flushing log appenders...")
      loggerContext.stop()

      // Additional metric flushing can be added here
      // e.g., Prometheus, CloudWatch, etc.

    } catch {
      case ex: Exception =>
        // Use println since logger may be shut down
        System.err.println(s"Error flushing logs and metrics: ${ex.getMessage}")
    }
  }
}

object GracefulShutdown {

  /**
   * Create and register a graceful shutdown handler.
   *
   * @param spark SparkSession to manage
   * @param shutdownTimeoutSeconds Maximum time to wait for operations to complete
   * @return GracefulShutdown instance
   */
  def apply(spark: SparkSession, shutdownTimeoutSeconds: Int = 60): GracefulShutdown = {
    val handler = new GracefulShutdown(spark, shutdownTimeoutSeconds)
    handler.registerShutdownHook()
    handler
  }
}
