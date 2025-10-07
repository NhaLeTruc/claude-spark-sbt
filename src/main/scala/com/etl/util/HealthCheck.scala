package com.etl.util

import com.sun.net.httpserver.{HttpExchange, HttpHandler, HttpServer}
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory
import play.api.libs.json.{Json, JsObject}

import java.net.InetSocketAddress
import scala.util.{Failure, Success, Try}

/**
 * HTTP health check endpoint for ETL pipeline monitoring.
 * Provides liveness and readiness probes for orchestration systems (Kubernetes, ECS, etc.).
 *
 * Endpoints:
 * - GET /health/live: Liveness probe (is the application running?)
 * - GET /health/ready: Readiness probe (is the application ready to accept work?)
 * - GET /health: Combined health status
 *
 * Response format (JSON):
 * {
 *   "status": "healthy|unhealthy",
 *   "timestamp": 1234567890,
 *   "components": {
 *     "spark": "healthy|unhealthy",
 *     "application": "healthy|unhealthy"
 *   },
 *   "details": {
 *     "sparkVersion": "3.5.6",
 *     "appId": "local-1234567890",
 *     "activeJobs": 0
 *   }
 * }
 *
 * Usage:
 * {{{
 * val healthCheck = new HealthCheck(spark, port = 8888)
 * healthCheck.start()
 *
 * // ... run pipeline ...
 *
 * healthCheck.stop()
 * }}}
 */
class HealthCheck(
  spark: SparkSession,
  port: Int = 8888
) {
  private val logger = LoggerFactory.getLogger(getClass)
  private var server: Option[HttpServer] = None
  private var isReady: Boolean = false

  /**
   * Start the health check HTTP server.
   */
  def start(): Unit = {
    try {
      logger.info(s"Starting health check server on port $port")

      val httpServer = HttpServer.create(new InetSocketAddress(port), 0)

      // Register endpoints
      httpServer.createContext("/health/live", new LivenessHandler())
      httpServer.createContext("/health/ready", new ReadinessHandler())
      httpServer.createContext("/health", new HealthHandler())

      httpServer.setExecutor(null) // Use default executor
      httpServer.start()

      server = Some(httpServer)
      logger.info(s"Health check server started on port $port")

    } catch {
      case ex: Exception =>
        logger.error(s"Failed to start health check server: ${ex.getMessage}", ex)
    }
  }

  /**
   * Stop the health check HTTP server.
   */
  def stop(): Unit = {
    server.foreach { s =>
      logger.info("Stopping health check server")
      s.stop(0)
      server = None
      logger.info("Health check server stopped")
    }
  }

  /**
   * Mark the application as ready to accept work.
   */
  def markReady(): Unit = {
    isReady = true
    logger.info("Application marked as ready")
  }

  /**
   * Mark the application as not ready.
   */
  def markNotReady(): Unit = {
    isReady = false
    logger.info("Application marked as not ready")
  }

  /**
   * Liveness probe handler.
   * Returns 200 if application is alive (process is running).
   */
  private class LivenessHandler extends HttpHandler {
    override def handle(exchange: HttpExchange): Unit = {
      val response = Json.obj(
        "status" -> "alive",
        "timestamp" -> System.currentTimeMillis()
      )

      sendResponse(exchange, 200, response.toString())
    }
  }

  /**
   * Readiness probe handler.
   * Returns 200 if application is ready to accept work, 503 otherwise.
   */
  private class ReadinessHandler extends HttpHandler {
    override def handle(exchange: HttpExchange): Unit = {
      val (statusCode, status) = if (isReady) (200, "ready") else (503, "not_ready")

      val response = Json.obj(
        "status" -> status,
        "timestamp" -> System.currentTimeMillis(),
        "ready" -> isReady
      )

      sendResponse(exchange, statusCode, response.toString())
    }
  }

  /**
   * Combined health check handler.
   * Returns detailed health status of all components.
   */
  private class HealthHandler extends HttpHandler {
    override def handle(exchange: HttpExchange): Unit = {
      val sparkHealth = checkSparkHealth()
      val appHealth = checkApplicationHealth()

      val overallHealthy = sparkHealth._1 && appHealth._1
      val statusCode = if (overallHealthy) 200 else 503

      val response = Json.obj(
        "status" -> (if (overallHealthy) "healthy" else "unhealthy"),
        "timestamp" -> System.currentTimeMillis(),
        "components" -> Json.obj(
          "spark" -> sparkHealth._2,
          "application" -> appHealth._2
        ),
        "details" -> getHealthDetails()
      )

      sendResponse(exchange, statusCode, response.toString())
    }
  }

  /**
   * Check Spark health.
   *
   * @return (isHealthy, status)
   */
  private def checkSparkHealth(): (Boolean, String) = {
    Try {
      !spark.sparkContext.isStopped
    } match {
      case Success(true) => (true, "healthy")
      case Success(false) => (false, "stopped")
      case Failure(_) => (false, "unhealthy")
    }
  }

  /**
   * Check application health.
   *
   * @return (isHealthy, status)
   */
  private def checkApplicationHealth(): (Boolean, String) = {
    if (isReady) {
      (true, "healthy")
    } else {
      (false, "not_ready")
    }
  }

  /**
   * Get detailed health information.
   *
   * @return JSON object with health details
   */
  private def getHealthDetails(): JsObject = {
    Try {
      val sc = spark.sparkContext
      val activeJobs = sc.statusTracker.getActiveJobIds().length
      val activeStages = sc.statusTracker.getActiveStageIds().length

      Json.obj(
        "sparkVersion" -> spark.version,
        "appId" -> sc.applicationId,
        "appName" -> sc.appName,
        "master" -> sc.master,
        "activeJobs" -> activeJobs,
        "activeStages" -> activeStages,
        "executorCount" -> sc.statusTracker.getExecutorInfos.length
      )
    } match {
      case Success(details) => details
      case Failure(ex) =>
        logger.warn(s"Failed to get health details: ${ex.getMessage}")
        Json.obj("error" -> ex.getMessage)
    }
  }

  /**
   * Send HTTP response.
   *
   * @param exchange HTTP exchange
   * @param statusCode HTTP status code
   * @param responseBody Response body
   */
  private def sendResponse(exchange: HttpExchange, statusCode: Int, responseBody: String): Unit = {
    Try {
      val responseBytes = responseBody.getBytes("UTF-8")
      exchange.getResponseHeaders.set("Content-Type", "application/json")
      exchange.sendResponseHeaders(statusCode, responseBytes.length)

      val os = exchange.getResponseBody
      try {
        os.write(responseBytes)
        os.flush()
      } finally {
        os.close()
      }
    } match {
      case Success(_) =>
        logger.debug(s"Sent response: status=$statusCode")

      case Failure(ex) =>
        logger.error(s"Failed to send response: ${ex.getMessage}", ex)
    }
  }
}

object HealthCheck {

  /**
   * Create and start a health check server.
   *
   * @param spark SparkSession
   * @param port Port to listen on (default: 8888)
   * @return HealthCheck instance
   */
  def apply(spark: SparkSession, port: Int = 8888): HealthCheck = {
    val healthCheck = new HealthCheck(spark, port)
    healthCheck.start()
    healthCheck
  }
}
