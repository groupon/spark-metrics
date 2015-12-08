/*
 * Copyright (c) 2016, Groupon, Inc.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are
 * met:
 *
 * Redistributions of source code must retain the above copyright notice,
 * this list of conditions and the following disclaimer.
 *
 * Redistributions in binary form must reproduce the above copyright
 * notice, this list of conditions and the following disclaimer in the
 * documentation and/or other materials provided with the distribution.
 *
 * Neither the name of GROUPON nor the names of its contributors may be
 * used to endorse or promote products derived from this software without
 * specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS
 * IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED
 * TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A
 * PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED
 * TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR
 * PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
 * LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
 * NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package org.apache.spark.groupon.metrics

import org.apache.spark.groupon.metrics.util.SparkContextSetup
import org.scalatest.{Matchers, FlatSpec}

class UserMetricsSystemTest extends FlatSpec with Matchers with SparkContextSetup {
  override def beforeAll(): Unit = {
    super.beforeAll()
    UserMetricsSystem.initialize(sc)
  }

  "initialize" should "be a no-op when called more than once" in {
    noException should be thrownBy {
      UserMetricsSystem.initialize(sc)
    }
  }

  "counter" should "create a new SparkCounter" in {
    UserMetricsSystem.counter("Counter") shouldBe a [SparkCounter]
  }

  "gauge" should "create a new SparkGauge" in {
    UserMetricsSystem.gauge("Gauge") shouldBe a [SparkGauge]
  }

  "histogram" should "create a new SparkHistogram" in {
    UserMetricsSystem.histogram("histogram") shouldBe a [SparkHistogram]
  }

  "meter" should "create a new SparkMeter" in {
    UserMetricsSystem.meter("Meter") shouldBe a [SparkMeter]
  }

  "timer" should "create a new SparkTimer" in {
    UserMetricsSystem.timer("Timer") shouldBe a [SparkTimer]
  }
}
