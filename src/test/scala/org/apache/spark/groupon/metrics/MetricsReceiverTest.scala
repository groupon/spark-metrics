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

import com.codahale.metrics._
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.concurrent.Eventually
import org.scalatest.{Matchers, BeforeAndAfter, FlatSpec}

class MetricsReceiverTest extends FlatSpec with Matchers with BeforeAndAfter with Eventually {
  private val master = "local[2]"
  private val appName = "test"
  private val sparkConf = new SparkConf().setAppName(appName).setMaster(master)
  var sc: SparkContext = _
  var metricsReceiver: MetricsReceiver = _

  before {
    // Since these tests interact with Spark's internal metrics system, we can't reuse Spark contexts
    sc = new SparkContext(sparkConf)
    sc.setLogLevel("WARN")
    metricsReceiver = new MetricsReceiver(sc, "TestNamespace")
    sc.env.rpcEnv.setupEndpoint("TestEndpoint", metricsReceiver)
  }

  after {
    metricsReceiver.stop()
    sc.stop()
  }

  def testReceiverContainsSingleMetric(name: String, metric: Metric): Unit = {
    metricsReceiver.metrics should have size 1
    metricsReceiver.metrics should contain key name
    metricsReceiver.metrics should contain value metric
  }

  "getOrCreateCounter" should "create a Counter on the first call and fetch the same one on subsequent calls" in {
    val counterName = "Counter"
    val counter = metricsReceiver.getOrCreateCounter(counterName)
    testReceiverContainsSingleMetric(counterName, counter)
    val counter2 = metricsReceiver.getOrCreateCounter(counterName)
    testReceiverContainsSingleMetric(counterName, counter2)

    counter shouldBe counter2
  }

  "getOrCreateHistogram" should "create a Histogram on the first call and fetch the same one on subsequent calls" in {
    val histogramName = "Histogram"
    val histogram = metricsReceiver.getOrCreateHistogram(histogramName, ReservoirClass.ExponentiallyDecaying)
    testReceiverContainsSingleMetric(histogramName, histogram)
    val histogram2 = metricsReceiver.getOrCreateHistogram(histogramName, ReservoirClass.ExponentiallyDecaying)
    testReceiverContainsSingleMetric(histogramName, histogram2)

    histogram shouldBe histogram2
  }

  "getOrCreateMeter" should "create a Meter on the first call and fetch the same one on subsequent calls" in {
    val meterName = "Meter"
    val meter = metricsReceiver.getOrCreateMeter(meterName)
    testReceiverContainsSingleMetric(meterName, meter)
    val meter2 = metricsReceiver.getOrCreateMeter(meterName)
    testReceiverContainsSingleMetric(meterName, meter2)

    meter shouldBe meter2
  }

  "getOrCreateTimer" should "create a Timer on the first call and fetch the same one on subsequent calls" in {
    val timerName = "Timer"
    val timer = metricsReceiver.getOrCreateTimer(timerName, ReservoirClass.ExponentiallyDecaying, ClockClass.UserTime)
    testReceiverContainsSingleMetric(timerName, timer)
    val timer2 = metricsReceiver.getOrCreateTimer(timerName, ReservoirClass.ExponentiallyDecaying, ClockClass.UserTime)
    testReceiverContainsSingleMetric(timerName, timer2)

    timer shouldBe timer2
  }

  "createGaugeIfAbsent" should "create a Gauge on the first call and do nothing on subsequent calls" in {
    val gaugeName = "Gauge"
    val gaugeValue = 0
    metricsReceiver.lastGaugeValues.put(gaugeName, gaugeValue)

    val gauge = metricsReceiver.getOrCreateGauge(gaugeName)
    testReceiverContainsSingleMetric(gaugeName, gauge)
    val gauge2 = metricsReceiver.getOrCreateGauge(gaugeName)
    testReceiverContainsSingleMetric(gaugeName, gauge2)

    gauge shouldBe gauge2
  }

  "receive" should "handle CounterMessage correctly" in {
    val counterName = "Counter"
    val counterMessage = CounterMessage(counterName, 1)
    metricsReceiver.self.send(counterMessage)

    eventually {
      metricsReceiver.metrics should contain key counterName
      metricsReceiver.metrics.get(counterName).asInstanceOf[Counter].getCount shouldBe 1
    }

    metricsReceiver.self.send(counterMessage)

    eventually {
      metricsReceiver.metrics.get(counterName).asInstanceOf[Counter].getCount shouldBe 2
    }
  }

  it should "handle HistogramMessage correctly" in {
    val histogramName = "Histogram"
    val histogramMessage = HistogramMessage(histogramName, 1, ReservoirClass.ExponentiallyDecaying)
    metricsReceiver.self.send(histogramMessage)

    eventually {
      metricsReceiver.metrics should contain key histogramName
      metricsReceiver.metrics.get(histogramName).asInstanceOf[Histogram].getCount shouldBe 1
    }

    metricsReceiver.self.send(histogramMessage)

    eventually {
      metricsReceiver.metrics.get(histogramName).asInstanceOf[Histogram].getCount shouldBe 2
    }
  }

  it should "handle MeterMessage correctly" in {
    val meterName = "Meter"
    val meterMessage = MeterMessage(meterName, 1)
    metricsReceiver.self.send(meterMessage)

    eventually {
      metricsReceiver.metrics should contain key meterName
      metricsReceiver.metrics.get(meterName).asInstanceOf[Meter].getCount shouldBe 1
    }

    metricsReceiver.self.send(meterMessage)

    eventually {
      metricsReceiver.metrics.get(meterName).asInstanceOf[Meter].getCount shouldBe 2
    }
  }

  it should "handle TimerMessage correctly" in {
    val timerName = "Timer"
    val timerMessage = TimerMessage(timerName, 1, ReservoirClass.ExponentiallyDecaying, ClockClass.UserTime)
    metricsReceiver.self.send(timerMessage)

    eventually {
      metricsReceiver.metrics should contain key timerName
      metricsReceiver.metrics.get(timerName).asInstanceOf[Timer].getCount shouldBe 1
    }

    metricsReceiver.self.send(timerMessage)

    eventually {
      metricsReceiver.metrics.get(timerName).asInstanceOf[Timer].getCount shouldBe 2
    }
  }

  it should "handle GaugeMessage correctly" in {
    val gaugeName = "Gauge"
    metricsReceiver.self.send(GaugeMessage(gaugeName, 1))

    eventually {
      metricsReceiver.metrics should contain key gaugeName
      metricsReceiver.metrics.get(gaugeName).asInstanceOf[Gauge[AnyVal]].getValue shouldBe 1
    }

    metricsReceiver.self.send(GaugeMessage(gaugeName, 2))

    eventually {
      metricsReceiver.metrics.get(gaugeName).asInstanceOf[Gauge[AnyVal]].getValue shouldBe 2
    }
  }
}
