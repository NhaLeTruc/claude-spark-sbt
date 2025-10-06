package com.etl.unit.util

import com.etl.util.Logging
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.slf4j.MDC

class LoggingSpec extends AnyFlatSpec with Matchers {

  class TestLogging extends Logging {
    def testLogInfo(message: String, context: Map[String, String] = Map.empty): Unit = {
      logInfo(message, context)
    }

    def testLogWarn(message: String, context: Map[String, String] = Map.empty): Unit = {
      logWarn(message, context)
    }

    def testLogError(message: String, error: Option[Throwable] = None, context: Map[String, String] = Map.empty): Unit = {
      logError(message, error, context)
    }

    def testWithMDC[A](context: Map[String, String])(block: => A): A = {
      withMDC(context)(block)
    }

    def testGetCurrentMDC: Map[String, String] = {
      getCurrentMDC
    }
  }

  "Logging" should "log info messages" in {
    val logging = new TestLogging

    // Should not throw exception
    noException should be thrownBy {
      logging.testLogInfo("Test info message")
    }
  }

  it should "log with MDC context" in {
    val logging = new TestLogging
    val context = Map(
      "pipelineId" -> "test-pipeline",
      "traceId"    -> "trace-123"
    )

    // When logging with context
    noException should be thrownBy {
      logging.testLogInfo("Test with context", context)
    }
  }

  it should "set and clear MDC context" in {
    val logging = new TestLogging
    val context = Map("key1" -> "value1", "key2" -> "value2")

    // Before: MDC should be empty
    MDC.clear()
    Option(MDC.get("key1")) shouldBe None

    // When: Execute block with MDC
    val result = logging.testWithMDC(context) {
      // Inside block: MDC should have values
      Option(MDC.get("key1")) shouldBe Some("value1")
      Option(MDC.get("key2")) shouldBe Some("value2")
      "result"
    }

    // After: MDC should be cleared
    Option(MDC.get("key1")) shouldBe None
    Option(MDC.get("key2")) shouldBe None
    result shouldBe "result"
  }

  it should "preserve existing MDC values" in {
    val logging = new TestLogging

    // Given: Some existing MDC value
    MDC.clear()
    MDC.put("existing", "value")

    // When: Add more MDC values in nested block
    logging.testWithMDC(Map("new" -> "value2")) {
      Option(MDC.get("existing")) shouldBe Some("value")
      Option(MDC.get("new")) shouldBe Some("value2")
    }

    // After: Original MDC restored
    Option(MDC.get("existing")) shouldBe Some("value")
    Option(MDC.get("new")) shouldBe None

    MDC.clear()
  }

  it should "log error messages with exceptions" in {
    val logging = new TestLogging
    val exception = new RuntimeException("Test error")

    noException should be thrownBy {
      logging.testLogError("Error occurred", Some(exception))
    }
  }

  it should "log warn messages" in {
    val logging = new TestLogging

    noException should be thrownBy {
      logging.testLogWarn("Warning message")
    }
  }

  it should "retrieve current MDC context" in {
    val logging = new TestLogging

    MDC.clear()
    MDC.put("key1", "value1")
    MDC.put("key2", "value2")

    val currentMDC = logging.testGetCurrentMDC

    currentMDC should contain("key1" -> "value1")
    currentMDC should contain("key2" -> "value2")

    MDC.clear()
  }

  it should "handle empty MDC context" in {
    val logging = new TestLogging

    MDC.clear()
    val result = logging.testWithMDC(Map.empty) {
      "result"
    }

    result shouldBe "result"
  }
}
