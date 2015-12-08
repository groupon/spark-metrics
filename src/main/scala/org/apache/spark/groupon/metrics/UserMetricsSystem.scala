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

import com.codahale.metrics.{Clock, Reservoir}
import org.apache.spark.util.RpcUtils
import org.apache.spark.{SparkContext, SparkEnv}

/**
 * Entry point for collecting user-defined metrics.
 */
object UserMetricsSystem {
  val DefaultNamespace = "UserMetrics"

  // Used to ensure that `initialize()` is executed only once in the driver.
  private var initialized = false

  /*
  This is to handle cases when some Spark code uses the UserMetricsSystem. The potential problems could be either:

    1. No active SparkContext. This most likely just means that a SparkMetric instance was instantiated in some class
    for a Spark app during a test case.
    2. initialize() was not called on the UserMetricsSystem before a SparkMetric was created. This could again be some
    test case, or it could be that a Spark app that is using this library incorrectly.

  For either of these cases, the exceptions that initially get thrown are not that helpful, so converting these to more
  sensible exceptions should hopefully help users of this library
  */
  @transient private lazy val metricsEndpoint = Option(SparkEnv.get) match {
    case Some(sparkEnv) => {
      try {
        RpcUtils.makeDriverRef(MetricsReceiver.DefaultEndpointName, sparkEnv.conf, sparkEnv.rpcEnv)
      } catch {
        case e: Exception => {
          throw new NotInitializedException(s"Could not instantiate SparkMetric instance. Please ensure " +
            "UserMetricsSystem.initialize() was invoked in the driver before the SparkContext was started.", e)
        }
      }
    }
    case None => {
      throw new SparkContextNotFoundException("Could not retrieve SparkEnv instance. This means that there was no " +
        "running SparkContext when UserMetricsSystem was used.")
    }
  }

  /**
   * Initialize the metrics system.
   *
   * Must be invoked in the driver before the SparkContext is started.
   *
   * @param sparkContext app's [[SparkContext]]
   * @param metricNamespace namespace of metrics used for publishing. By default, it is `UserMetrics`.
   */
  def initialize(sparkContext: SparkContext, metricNamespace: String = DefaultNamespace): Unit = {
    if (!initialized) {
      sparkContext.env.rpcEnv.setupEndpoint(
        MetricsReceiver.DefaultEndpointName,
        new MetricsReceiver(sparkContext, metricNamespace)
      )
      initialized = true
    }
  }

  /**
   * Create a [[SparkCounter]] that pushes data points to the driver for aggregation.
   *
   * @param metricName name of the metric
   * @return the [[SparkCounter]] instance
   */
  def counter(metricName: String): SparkCounter = {
    new SparkCounter(metricsEndpoint, metricName)
  }

  /**
   * Create a [[SparkGauge]] that pushes data points to the driver for aggregation.
   *
   * @param metricName name of the metric
   * @return the [[SparkGauge]] instance
   */
  def gauge(metricName: String): SparkGauge = {
    new SparkGauge(metricsEndpoint, metricName)
  }

  /**
   * Create a [[SparkHistogram]] that pushes data points to the driver for aggregation.
   *
   * @param metricName name of the metric
   * @param reservoirClass [[Class]] of the [[Reservoir]] that backs the [[com.codahale.metrics.Histogram]] on the
   *                       driver. By default, it is the [[com.codahale.metrics.ExponentiallyDecayingReservoir]].
   * @return the [[SparkHistogram]] instance
   */
  def histogram(metricName: String,
                reservoirClass: Class[_ <: Reservoir] = ReservoirClass.ExponentiallyDecaying): SparkHistogram = {
    new SparkHistogram(metricsEndpoint, metricName, reservoirClass)
  }

  /**
   * Create a [[SparkMeter]] that pushes data points to the driver for aggregation.
   *
   * @param metricName name of the metric
   * @return the [[SparkMeter]] instance
   */
  def meter(metricName: String): SparkMeter = {
    new SparkMeter(metricsEndpoint, metricName)
  }

  /**
   * Create a [[SparkTimer]] that pushes data points to the driver for aggregation.
   *
   * @param metricName name of the metric
   * @param reservoirClass [[Class]] of the [[Reservoir]] that backs the [[com.codahale.metrics.Timer]] on the
   *                       driver. By default, it is the [[com.codahale.metrics.ExponentiallyDecayingReservoir]].
   * @param clockClass [[Class]] of the [[Clock]] that backs the [[com.codahale.metrics.Timer]] on the driver. By
   *                  default, it is the [[com.codahale.metrics.Clock.UserTimeClock]]
   * @return the [[SparkTimer]] instance
   */
  def timer(metricName: String,
            reservoirClass: Class[_ <: Reservoir] = ReservoirClass.ExponentiallyDecaying,
            clockClass: Class[_ <: Clock] = ClockClass.UserTime): SparkTimer = {
    new SparkTimer(metricsEndpoint, metricName, reservoirClass, clockClass)
  }
}

case class NotInitializedException(message: String, cause: Throwable) extends Exception(message, cause)

case class SparkContextNotFoundException(message: String) extends Exception(message)
