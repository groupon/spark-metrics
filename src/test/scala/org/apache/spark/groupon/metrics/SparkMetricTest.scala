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

import java.util.concurrent.TimeUnit

import org.apache.spark.groupon.metrics.util.{TestMetricsRpcEndpoint, SparkContextSetup}
import org.apache.spark.rpc.RpcEndpointRef
import org.scalatest.concurrent.Eventually
import org.scalatest.{Matchers, BeforeAndAfter, FlatSpec}

import scala.util.Random

class SparkMetricTest extends FlatSpec with Matchers with BeforeAndAfter with Eventually with SparkContextSetup {
  var metricsEndpoint: TestMetricsRpcEndpoint = _
  var metricsEndpointRef: RpcEndpointRef = _

  before {
    metricsEndpoint = new TestMetricsRpcEndpoint(sc.env.rpcEnv)
    metricsEndpointRef = sc.env.rpcEnv.setupEndpoint(Random.alphanumeric.take(32).mkString(""), metricsEndpoint)
  }

  after {
    metricsEndpoint.stop()
    metricsEndpoint = null
    metricsEndpointRef = null
  }

  "Counter" should "increment correctly" in {
    val counterName = "counter"
    val counter = new SparkCounter(metricsEndpointRef, counterName)
    val incrementVals = Seq(1, 2, 3, -1, -2, -3)
    incrementVals.foreach(i => counter.inc(i))

    eventually {
      metricsEndpoint.getMetricNames shouldBe Seq.fill(incrementVals.length)(counterName)
      metricsEndpoint.getMetricValues shouldBe incrementVals
    }
  }

  it should "decrement correctly" in {
    val counterName = "counter"
    val counter = new SparkCounter(metricsEndpointRef, counterName)
    val decrementVals = Seq(1, 2, 3, -1, -2, -3)
    decrementVals.foreach(i => counter.dec(i))

    eventually {
      metricsEndpoint.getMetricNames shouldBe Seq.fill(decrementVals.length)(counterName)
      metricsEndpoint.getMetricValues shouldBe decrementVals.map(i => -i)
    }
  }

  "Gauge" should "set its value correctly" in {
    val gaugeName = "gauge"
    val gauge = new SparkGauge(metricsEndpointRef, gaugeName)
    val gaugeVal = 1
    gauge.set(gaugeVal)

    eventually {
      metricsEndpoint.getMetricNames shouldBe Seq(gaugeName)
      metricsEndpoint.getMetricValues shouldBe Seq(gaugeVal)
    }
  }

  "Histogram" should "set its value correctly" in {
    val histogramName = "histogram"
    val histogram = new SparkHistogram(metricsEndpointRef, histogramName, ReservoirClass.ExponentiallyDecaying)
    val histogramVals = Seq(1, 2, 3, -1, -2, -3)
    histogramVals.foreach(i => histogram.update(i))

    eventually {
      metricsEndpoint.getMetricNames shouldBe Seq.fill(histogramVals.length)(histogramName)
      metricsEndpoint.getMetricValues shouldBe histogramVals
    }
  }

  it should "use the correct Reservoir class" in {
    val reservoirClasses = Seq(
      ReservoirClass.ExponentiallyDecaying,
      ReservoirClass.SlidingTimeWindow,
      ReservoirClass.SlidingWindow,
      ReservoirClass.Uniform
    )
    reservoirClasses.foreach(reservoirClass => {
      new SparkHistogram(metricsEndpointRef, "histogram", reservoirClass).update(0)
    })

    eventually {
      metricsEndpoint.metricStore.map(metricMessage => {
        metricMessage.asInstanceOf[HistogramMessage].reservoirClass
      }) shouldBe reservoirClasses
    }
  }

  "Meter" should "set its value correctly" in {
    val meterName = "meter"
    val meter = new SparkMeter(metricsEndpointRef, meterName)
    val meterVals = Seq(1, 2, 3, -1, -2, -3)
    meterVals.foreach(i => meter.mark(i))

    eventually {
      metricsEndpoint.getMetricNames shouldBe Seq.fill(meterVals.length)(meterName)
      metricsEndpoint.getMetricValues shouldBe meterVals
    }
  }

  "Timer" should "set its value correctly with 'update'" in {
    val timerName = "timer"
    val timer = new SparkTimer(metricsEndpointRef, timerName, ReservoirClass.ExponentiallyDecaying, ClockClass.UserTime)
    val timerVal = 1000
    timer.update(timerVal, TimeUnit.NANOSECONDS)

    eventually {
      metricsEndpoint.getMetricNames shouldBe Seq(timerName)
      metricsEndpoint.getMetricValues shouldBe Seq(timerVal)
    }
  }

  it should "convert time units correctly with 'update'" in {
    val timer = new SparkTimer(metricsEndpointRef, "timer", ReservoirClass.ExponentiallyDecaying, ClockClass.UserTime)
    val timeUnits = Seq(
      TimeUnit.DAYS,
      TimeUnit.HOURS,
      TimeUnit.MICROSECONDS,
      TimeUnit.MILLISECONDS,
      TimeUnit.MINUTES,
      TimeUnit.NANOSECONDS,
      TimeUnit.SECONDS
    )
    val timerVal = 1
    timeUnits.foreach(timeUnit => timer.update(timerVal, timeUnit))

    eventually {
      metricsEndpoint.getMetricValues shouldBe timeUnits.map(timeUnit => timeUnit.toNanos(timerVal))
    }
  }

  it should "time a function correctly" in {
    val timer = new SparkTimer(metricsEndpointRef, "timer", ReservoirClass.ExponentiallyDecaying, ClockClass.UserTime)
    // Function should take about 500 milliseconds
    val (duration, timeUnit) = (500, TimeUnit.MILLISECONDS)
    val toleranceRange = timeUnit.toNanos(duration) +- timeUnit.toNanos(100)
    timer.time({
      timeUnit.sleep(duration)
    })

    eventually {
      metricsEndpoint.getMetricValues.head.asInstanceOf[Long] shouldBe toleranceRange
    }
  }

  it should "time using a Context correctly" in {
    val timer = new SparkTimer(metricsEndpointRef, "timer", ReservoirClass.ExponentiallyDecaying, ClockClass.UserTime)
    // Timer should measure about 500 milliseconds
    val (duration, timeUnit) = (500, TimeUnit.MILLISECONDS)
    val toleranceRange = timeUnit.toNanos(duration) +- timeUnit.toNanos(100)
    val timerContext = timer.time()
    timeUnit.sleep(duration)
    timerContext.stop()

    eventually {
      metricsEndpoint.getMetricValues.head.asInstanceOf[Long] shouldBe toleranceRange
    }
  }

  it should "use the correct Reservoir class" in {
    val reservoirClasses = Seq(
      ReservoirClass.ExponentiallyDecaying,
      ReservoirClass.SlidingTimeWindow,
      ReservoirClass.SlidingWindow,
      ReservoirClass.Uniform
    )
    reservoirClasses.foreach(reservoirClass => {
      new SparkTimer(metricsEndpointRef, "timer", reservoirClass, ClockClass.UserTime).update(0, TimeUnit.SECONDS)
    })

    eventually {
      metricsEndpoint.metricStore.map(metricMessage => {
        metricMessage.asInstanceOf[TimerMessage].reservoirClass
      }) shouldBe reservoirClasses
    }
  }

  it should "use the correct Clock class" in {
    val clockClasses = Seq(
      ClockClass.UserTime,
      ClockClass.CpuTime
    )
    clockClasses.foreach(clockClass => {
      new SparkTimer(metricsEndpointRef, "timer", ReservoirClass.ExponentiallyDecaying, clockClass)
        .update(0, TimeUnit.SECONDS)
    })

    eventually {
      metricsEndpoint.metricStore.map(metricMessage => {
        metricMessage.asInstanceOf[TimerMessage].clockClass
      }) shouldBe clockClasses
    }
  }
}
