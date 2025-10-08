package com.etl.monitoring

import com.sun.net.httpserver.{HttpExchange, HttpHandler, HttpServer}
import org.slf4j.LoggerFactory

import java.net.InetSocketAddress
import scala.util.{Failure, Success, Try}

/**
 * HTTP server for exposing Prometheus metrics.
 *
 * Provides a /metrics endpoint that returns all registered metrics in
 * Prometheus text format, ready to be scraped by Prometheus.
 *
 * Endpoints:
 * - GET /metrics: Returns all metrics in Prometheus exposition format
 * - GET /metrics/health: Simple health check for the metrics server
 *
 * Usage:
 * {{{
 * val metricsServer = new MetricsHttpServer(port = 9090)
 * metricsServer.start()
 *
 * // ... application runs, metrics are collected ...
 *
 * metricsServer.stop()
 * }}}
 *
 * Prometheus Configuration:
 * {{{
 * scrape_configs:
 *   - job_name: 'etl-pipeline'
 *     static_configs:
 *       - targets: ['localhost:9090']
 *     scrape_interval: 15s
 * }}}
 *
 * @param port Port to listen on (default: 9090, Prometheus standard)
 */
class MetricsHttpServer(port: Int = 9090) {
  private val logger = LoggerFactory.getLogger(getClass)
  private var server: Option[HttpServer] = None
  private val startTime: Long = System.currentTimeMillis()

  /**
   * Start the metrics HTTP server.
   */
  def start(): Unit = {
    try {
      logger.info(s"Starting metrics server on port $port")

      val httpServer = HttpServer.create(new InetSocketAddress(port), 0)

      // Register endpoints
      httpServer.createContext("/metrics", new MetricsHandler())
      httpServer.createContext("/metrics/health", new MetricsHealthHandler())

      httpServer.setExecutor(null) // Use default executor
      httpServer.start()

      server = Some(httpServer)
      logger.info(s"Metrics server started on port $port")
      logger.info(s"Prometheus metrics available at http://localhost:$port/metrics")

    } catch {
      case ex: Exception =>
        logger.error(s"Failed to start metrics server: ${ex.getMessage}", ex)
        throw ex
    }
  }

  /**
   * Stop the metrics HTTP server.
   */
  def stop(): Unit = {
    server.foreach { s =>
      logger.info("Stopping metrics server")
      s.stop(0)
      server = None
      logger.info("Metrics server stopped")
    }
  }

  /**
   * Check if server is running.
   *
   * @return true if server is running, false otherwise
   */
  def isRunning: Boolean = server.isDefined

  /**
   * Metrics endpoint handler.
   *
   * Returns all registered metrics in Prometheus text format.
   */
  private class MetricsHandler extends HttpHandler {
    override def handle(exchange: HttpExchange): Unit = {
      Try {
        logger.debug("Serving /metrics request")

        // Export all metrics
        val metricsText = MetricsRegistry.exportMetrics()

        // Send response
        val responseBytes = metricsText.getBytes("UTF-8")
        exchange.getResponseHeaders.set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
        exchange.sendResponseHeaders(200, responseBytes.length)

        val os = exchange.getResponseBody
        try {
          os.write(responseBytes)
          os.flush()
        } finally {
          os.close()
        }

        logger.debug(s"Served ${responseBytes.length} bytes of metrics")

      } match {
        case Success(_) =>
          // Successfully handled request

        case Failure(ex) =>
          logger.error(s"Failed to handle /metrics request: ${ex.getMessage}", ex)
          sendErrorResponse(exchange, 500, s"Internal error: ${ex.getMessage}")
      }
    }
  }

  /**
   * Health check endpoint for the metrics server.
   *
   * Returns simple JSON health status.
   */
  private class MetricsHealthHandler extends HttpHandler {
    override def handle(exchange: HttpExchange): Unit = {
      Try {
        val uptimeSeconds = (System.currentTimeMillis() - startTime) / 1000
        val metricCount = MetricsRegistry.metricCount

        val response =
          s"""|{
              |  "status": "healthy",
              |  "timestamp": ${System.currentTimeMillis()},
              |  "uptime_seconds": $uptimeSeconds,
              |  "registered_metrics": $metricCount
              |}""".stripMargin

        val responseBytes = response.getBytes("UTF-8")
        exchange.getResponseHeaders.set("Content-Type", "application/json")
        exchange.sendResponseHeaders(200, responseBytes.length)

        val os = exchange.getResponseBody
        try {
          os.write(responseBytes)
          os.flush()
        } finally {
          os.close()
        }

      } match {
        case Success(_) =>
          // Successfully handled request

        case Failure(ex) =>
          logger.error(s"Failed to handle /metrics/health request: ${ex.getMessage}", ex)
          sendErrorResponse(exchange, 500, s"Internal error: ${ex.getMessage}")
      }
    }
  }

  /**
   * Send HTTP error response.
   *
   * @param exchange HTTP exchange
   * @param statusCode HTTP status code
   * @param message Error message
   */
  private def sendErrorResponse(exchange: HttpExchange, statusCode: Int, message: String): Unit = {
    Try {
      val responseBytes = message.getBytes("UTF-8")
      exchange.getResponseHeaders.set("Content-Type", "text/plain")
      exchange.sendResponseHeaders(statusCode, responseBytes.length)

      val os = exchange.getResponseBody
      try {
        os.write(responseBytes)
        os.flush()
      } finally {
        os.close()
      }
    } match {
      case Failure(ex) =>
        logger.error(s"Failed to send error response: ${ex.getMessage}", ex)
      case _ => // Successfully sent error response
    }
  }
}

object MetricsHttpServer {

  /**
   * Create and start a metrics HTTP server.
   *
   * @param port Port to listen on (default: 9090)
   * @return MetricsHttpServer instance
   */
  def apply(port: Int = 9090): MetricsHttpServer = {
    val server = new MetricsHttpServer(port)
    server.start()
    server
  }

  /**
   * Start metrics server with port from environment variable or config.
   *
   * Looks for METRICS_PORT env var, defaults to 9090 if not set.
   *
   * @return MetricsHttpServer instance
   */
  def fromEnv(): MetricsHttpServer = {
    val port = sys.env.get("METRICS_PORT").map(_.toInt).getOrElse(9090)
    apply(port)
  }
}
