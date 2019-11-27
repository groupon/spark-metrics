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

import java.io.Closeable
import java.util.concurrent.TimeUnit

import com.codahale.metrics.{Clock, Reservoir, Metric}
import com.codahale.metrics.{ExponentiallyDecayingReservoir, SlidingTimeWindowReservoir, SlidingWindowReservoir, UniformReservoir}
import org.apache.spark.rpc.RpcEndpointRef

/**
 * SparkMetric instances implement APIs that look like their Codahale counterparts, but do not store any state. Instead,
 * they just send the data points to the driver, where they are aggregated and published.
 */
sealed trait SparkMetric extends Metric with Serializable {
  @transient protected val metricsEndpoint: RpcEndpointRef

  /**
   * Send a metric data point to the driver. Does not wait for a response from the driver endpoint.
   * @param message [[MetricMessage]] to send
   */
  def sendMetric(message: MetricMessage): Unit = {
    metricsEndpoint.send(message)
  }
}

/**
 * Acts like a [[com.codahale.metrics.Counter]]
 */
class SparkCounter private[metrics] (
    override val metricsEndpoint: RpcEndpointRef,
    val metricName: String)
  extends SparkMetric {

  /**
   * Increment the counter by `n`. Default is 1.
   *
   * @param n amount by which the counter will be increased.
   */
  def inc(n: Long = 1): Unit = {
    sendMetric(CounterMessage(metricName, n))
  }

  /**
   * Decrement the counter by `n`. Default is 1.
   *
   * @param n amount by which the counter will be decreased.
   */
  def dec(n: Long = -1): Unit = {
    inc(-n)
  }
}

/**
 * Acts like a [[com.codahale.metrics.Gauge]].
 *
 * Unlike the Codahale version that requires an implementation of a `getValue()` method, this version implements a `set`
 * method that just sets the value of this gauge.
 */
class SparkGauge private[metrics] (
    override val metricsEndpoint: RpcEndpointRef,
    val metricName: String)
  extends SparkMetric {

  /**
   * Set the value of this gauge.
   *
   * @param value value to set this gauge to.
   */
  def set(value: AnyVal): Unit = {
    sendMetric(GaugeMessage(metricName, value))
  }

  def remove(): Unit = {
    sendMetric(Remove(metricName, "".asInstanceOf[AnyVal]))
  }
}

/**
 * Acts like a [[com.codahale.metrics.Histogram]]
 */
class SparkHistogram private[metrics] (
   override val metricsEndpoint: RpcEndpointRef,
   val metricName: String,
   val reservoirClass: Class[_ <: Reservoir])
  extends SparkMetric {

  /**
   * Add a recorded value.
   *
   * @param value value to record.
   */
  def update(value: Long): Unit = {
    sendMetric(HistogramMessage(metricName, value, reservoirClass))
  }

  /**
   * Add a recorded value.
   *
   * @param value value to record.
   */
  def update(value: Int): Unit = {
    update(value.toLong)
  }

  def remove(): Unit = {
    sendMetric(Remove(metricName, "".asInstanceOf[AnyVal]))
  }
}

/**
 * Acts like a [[com.codahale.metrics.Meter]]
 */
class SparkMeter private[metrics] (
    override val metricsEndpoint: RpcEndpointRef,
    val metricName: String)
  extends SparkMetric {

  /**
   * Mark the occurrence of a given number of events. Default is 1.
   *
   * @param count number of events.
   */
  def mark(count: Long = 1): Unit = {
    sendMetric(MeterMessage(metricName, count))
  }
}

class SparkTimer private[metrics] (
    override val metricsEndpoint: RpcEndpointRef,
    val metricName: String,
    val reservoirClass: Class[_ <: Reservoir],
    val clockClass: Class[_ <: Clock])
  extends SparkMetric {
  val ClockTimeUnit = TimeUnit.NANOSECONDS

  class Context(val timer: SparkTimer, val clock: Clock) extends Closeable {
    val startTime = clock.getTick

    /**
     * Update the timer with the difference between current and start time.
     *
     * Call to this method will not reset the start time. Multiple calls result in multiple updates.
     *
     * @return the elapsed time in nanoseconds
     */
    def stop(): Long = {
      val elapsed = clock.getTick - startTime
      timer.update(elapsed, ClockTimeUnit)
      elapsed
    }

    override def close(): Unit = stop()
  }

  @transient lazy val clock = clockClass.newInstance()

  /**
   * Add a recorded duration.
   *
   * @param duration length of the duration.
   * @param timeUnit [[TimeUnit]] of `duration`.
   */
  def update(duration: Long, timeUnit: TimeUnit): Unit = {
    sendMetric(TimerMessage(metricName, timeUnit.toNanos(duration), reservoirClass, clockClass))
  }

  /**
   * Create a new [[Context]]
   *
   * @return a new [[Context]]
   */
  def time(): Context = new Context(this, clock)

  /**
   * Time and record the duration of a function.
   *
   * @param f the function to time
   * @tparam T the return type of `f`
   * @return the value returned by `f`
   */
  def time[T](f: => T): T = {
    val startTime = clock.getTick
    try {
      f
    } finally {
      update(clock.getTick - startTime, ClockTimeUnit)
    }
  }
}

object ClockClass {
  val UserTime = classOf[Clock.UserTimeClock]
  val CpuTime = classOf[Clock.CpuTimeClock]
}

object ReservoirClass {
  val ExponentiallyDecaying = classOf[ExponentiallyDecayingReservoir]
  val SlidingTimeWindow = classOf[SlidingTimeWindowReservoir]
  val SlidingWindow = classOf[SlidingWindowReservoir]
  val Uniform = classOf[UniformReservoir]
}
