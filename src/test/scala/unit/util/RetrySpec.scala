package com.etl.unit.util

import com.etl.util.Retry
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.util.{Failure, Success, Try}

class RetrySpec extends AnyFlatSpec with Matchers {

  "Retry" should "succeed on first attempt" in {
    // Given an operation that succeeds immediately
    var attempts = 0
    def operation: String = {
      attempts += 1
      "success"
    }

    // When executing with retry
    val result = Retry.withRetry(maxAttempts = 3, delayMillis = 100)(operation)

    // Then should succeed on first attempt
    result shouldBe Right("success")
    attempts shouldBe 1
  }

  it should "succeed after retries" in {
    // Given an operation that fails twice then succeeds
    var attempts = 0
    def operation: String = {
      attempts += 1
      if (attempts < 3) throw new RuntimeException(s"Attempt $attempts failed")
      else "success"
    }

    // When executing with retry
    val result = Retry.withRetry(maxAttempts = 3, delayMillis = 100)(operation)

    // Then should succeed after 3 attempts
    result shouldBe Right("success")
    attempts shouldBe 3
  }

  it should "fail after exhausting retries" in {
    // Given an operation that always fails
    var attempts = 0
    def operation: String = {
      attempts += 1
      throw new RuntimeException(s"Attempt $attempts failed")
    }

    // When executing with retry
    val result = Retry.withRetry(maxAttempts = 3, delayMillis = 100)(operation)

    // Then should fail after 3 attempts
    result.isLeft shouldBe true
    result.left.get.getMessage should include("Attempt 3 failed")
    attempts shouldBe 3
  }

  it should "respect delay between retries" in {
    // Given an operation that fails
    var attempts = 0
    val startTime = System.currentTimeMillis()

    def operation: String = {
      attempts += 1
      throw new RuntimeException("Always fails")
    }

    // When executing with 200ms delay
    Retry.withRetry(maxAttempts = 3, delayMillis = 200)(operation)
    val duration = System.currentTimeMillis() - startTime

    // Then total duration should be at least 400ms (2 delays between 3 attempts)
    duration should be >= 400L
    attempts shouldBe 3
  }

  it should "handle zero maxAttempts" in {
    // Given operation and zero retries
    var attempts = 0
    def operation: String = {
      attempts += 1
      throw new RuntimeException("Fails")
    }

    // When executing with 0 attempts
    val result = Retry.withRetry(maxAttempts = 0, delayMillis = 100)(operation)

    // Then should not execute
    result.isLeft shouldBe true
    attempts shouldBe 0
  }

  it should "return last error after all retries" in {
    // Given operations with different error messages
    var attempts = 0
    def operation: String = {
      attempts += 1
      throw new RuntimeException(s"Error at attempt $attempts")
    }

    // When executing
    val result = Retry.withRetry(maxAttempts = 3, delayMillis = 50)(operation)

    // Then should return last error message
    result.isLeft shouldBe true
    result.left.get.getMessage shouldBe "Error at attempt 3"
  }

  it should "work with Try-based operations" in {
    // Given a Try-based operation
    var attempts = 0
    def tryOperation: Try[String] = {
      attempts += 1
      if (attempts < 2) Failure(new RuntimeException("Fail"))
      else Success("Success")
    }

    // When wrapped in retry
    val result = Retry.withRetry(maxAttempts = 3, delayMillis = 50) {
      tryOperation.get // Convert Try to exception-throwing
    }

    // Then should succeed
    result shouldBe Right("Success")
    attempts shouldBe 2
  }
}
