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

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.TimeUnit
import java.util.function

import com.codahale.metrics._
import org.apache.spark.{SparkContext, SparkException}
import org.apache.spark.rpc.{RpcEndpoint, RpcEnv}

/**
 * MetricsReceiver is an [[RpcEndpoint]] on the driver node that collects data points for metrics from all the executors
 * and aggregates them using the Codahale metrics library.
 *
 * This class interacts with Spark's [[org.apache.spark.metrics.MetricsSystem]] class to add the [[Metric]] instances
 * created by this library to Spark's internal [[MetricRegistry]]. When the [[MetricsReceiver]] receives a data point
 * from a [[SparkMetric]] that hasn't been seen before (identified by the `metricName` field in the [[MetricMessage]]),
 * the [[MetricsReceiver]] creates an instance of a Codahale [[Metric]] that corresponds with the [[SparkMetric]].
 *
 * This new instance is then added to Spark's internal [[MetricRegistry]] via the
 * [[org.apache.spark.metrics.MetricsSystem.registerSource()]] method. This is the only available API to add a new
 * [[Metric]], but to add a [[org.apache.spark.metrics.source.Source]], we need to create a [[MetricRegistry]] as well.
 * In other words, to add a new [[Metric]] to the Spark [[org.apache.spark.metrics.MetricsSystem]], we need to wrap a
 * [[Metric]] in a [[MetricRegistry]], which is in turn wrapped by a [[org.apache.spark.metrics.source.Source]].
 *
 * The ideal implementation would be that the [[MetricsReceiver]] class has a single corresponding
 * [[org.apache.spark.metrics.source.Source]] instance that contains a [[MetricRegistry]] which holds all the [[Metric]]
 * instances created in the [[MetricsReceiver]]. Then, we could just register this one
 * [[org.apache.spark.metrics.source.Source]] with Spark, and all of this library's [[Metric]] instances will be
 * integrated with Spark.
 *
 * Unfortunately, this isn't possible due to the fact that Spark's internal [[MetricRegistry]] doesn't listen for
 * updates to external [[MetricRegistry]] instances that were added in the
 * [[org.apache.spark.metrics.MetricsSystem.registerSource()]] call. That method registers whatever [[Metric]] instances
 * are in the [[MetricRegistry]] of the [[org.apache.spark.metrics.source.Source]] at that time, but any future updates
 * to that [[MetricRegistry]] won't get propagated to Spark's internal [[MetricRegistry]].
 *
 * We could add a [[com.codahale.metrics.MetricRegistryListener]] to this library's [[MetricRegistry]], and whenever
 * there is an update to that, we could propagate these changes to Spark's [[MetricRegistry]]. This would be possible
 * if we had access to Spark's [[MetricRegistry]], but this is currently a private field in the
 * [[org.apache.spark.metrics.MetricsSystem]].
 *
 * @param sparkContext app's [[SparkContext]]
 * @param metricNamespace namespace of metrics used for publishing.
 */
private[metrics] class MetricsReceiver(val sparkContext: SparkContext,
                                       val metricNamespace: String) extends RpcEndpoint {
  override val rpcEnv: RpcEnv = sparkContext.env.rpcEnv

  // Tracks the last observed value for each Gauge
  val lastGaugeValues: ConcurrentHashMap[String, AnyVal] = new ConcurrentHashMap[String, AnyVal]()
  // Keeps track of all the Metric instances that are being published
  val metrics: ConcurrentHashMap[String, Metric] = new ConcurrentHashMap[String, Metric]()

  /**
   * Handle the data points pushed from the executors.
   *
   * Performs the appropriate update operations on the [[Metric]] instances. If a `metricName` is seen for the first
   * time, a [[Metric]] instance is created using the data from the [[MetricMessage]].
   */
  override def receive: PartialFunction[Any, Unit] = {
    case CounterMessage(metricName, value) =>
      getOrCreateCounter(metricName).inc(value)
    case HistogramMessage(metricName, value, reservoirClass) =>
      getOrCreateHistogram(metricName, reservoirClass).update(value)
    case MeterMessage(metricName, value) =>
      getOrCreateMeter(metricName).mark(value)
    case TimerMessage(metricName, value, reservoirClass, clockClass) =>
      getOrCreateTimer(metricName, reservoirClass, clockClass).update(value, MetricsReceiver.DefaultTimeUnit)
    case GaugeMessage(metricName, value) =>
      lastGaugeValues.put(metricName, value)
      getOrCreateGauge(metricName)
    case Remove(metricName, value) =>
      remove(metricName, value)
    case message: Any => throw new SparkException(s"$self does not implement 'receive' for message: $message")
  }

  def compute[T <: Metric](metricName: String, newMetric: => T): java.util.function.Function[String, Metric] = new function.Function[String, Metric] {
    override def apply(t: String): Metric = {
      registerMetricSource(metricName, newMetric)
      newMetric
    }
  }

  def removeMetrics[T <: Metric](metricName: String, newMetric: => T): java.util.function.Function[String, Metric] = new function.Function[String, Metric] {
    override def apply(t: String): Metric = {
      removeMetricsSource(metricName, newMetric)
      newMetric
    }
  }


  def getOrCreateCounter(metricName: String): Counter =
    metrics.computeIfAbsent(metricName, compute(metricName, new Counter)
    ).asInstanceOf[Counter]


  def getOrCreateHistogram(metricName: String, reservoirClass: Class[_ <: Reservoir]): Histogram =
    metrics.computeIfAbsent(
      metricName,
      compute(metricName, new Histogram(reservoirClass.newInstance()))
    ).asInstanceOf[Histogram]


  def getOrCreateMeter(metricName: String): Meter =
    metrics.computeIfAbsent(metricName, compute(metricName, new Meter)
    ).asInstanceOf[Meter]


  def getOrCreateTimer(metricName: String, reservoirClass: Class[_ <: Reservoir], clockClass: Class[_ <: Clock]): Timer =
    metrics.computeIfAbsent(
      metricName,
      compute(metricName, new Timer(reservoirClass.newInstance(), clockClass.newInstance()))
    ).asInstanceOf[Timer]

  def getOrCreateGauge(metricName: String): Gauge[AnyVal] = {
    metrics.computeIfAbsent(
      metricName,
      compute(metricName, new Gauge[AnyVal] {
        override def getValue: AnyVal = lastGaugeValues.get(metricName)
      })
    ).asInstanceOf[Gauge[AnyVal]]
  }

  def remove(name: String, value: AnyVal): Unit = {
    lastGaugeValues.remove(name)
    metrics.remove(name)
    metrics.computeIfAbsent(
      name,
      removeMetrics(name, new Gauge[AnyVal] {
        override def getValue: AnyVal = lastGaugeValues.get(name)
      })
    ).asInstanceOf[Gauge[AnyVal]]
    val metric = metrics.remove(name)
    if (metric != null) return true
    false
  }

  def removeMetricsSource(metricName: String, metric: Metric): Unit =  {
    sparkContext.env.metricsSystem.removeSource(
      new Source {
        override val sourceName = s"${sparkContext.appName}.$metricNamespace.$metricName"
        override def metricRegistry: MetricRegistry = {
          val metrics = new MetricRegistry
          metrics.remove(metricName)
          metrics
        }
      }
    )
  }

  /**
   * Register a [[Metric]] with Spark's [[org.apache.spark.metrics.MetricsSystem]].
   *
   * Since updates to an external [[MetricRegistry]] that is already registered with the
   * [[org.apache.spark.metrics.MetricsSystem]] aren't propagated to Spark's internal [[MetricRegistry]] instance, a new
   * [[MetricRegistry]] must be created for each new [[Metric]] that needs to be published.
   *
   * @param metricName name of the Metric
   * @param metric [[Metric]] instance to be published
   */
  def registerMetricSource(metricName: String, metric: Metric): Unit =  {
    sparkContext.env.metricsSystem.registerSource(
      new Source {
        override val sourceName = s"${sparkContext.appName}.$metricNamespace"
        override def metricRegistry: MetricRegistry = {
          val metrics = new MetricRegistry
          metrics.register(metricName, metric)
          metrics
        }
      }
    )
  }
}

private[metrics] object MetricsReceiver {
  val DefaultTimeUnit = TimeUnit.NANOSECONDS
  val DefaultEndpointName = "MetricsReceiver"
}
