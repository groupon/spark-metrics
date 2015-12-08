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

package org.apache.spark.groupon.metrics.example

import java.lang.management.ManagementFactory
import java.util.concurrent.{TimeUnit, ScheduledThreadPoolExecutor}

import org.apache.spark.SparkConf
import org.apache.spark.executor.TaskMetrics
import org.apache.spark.groupon.metrics.UserMetricsSystem
import org.apache.spark.scheduler.TaskInfo
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.streaming.scheduler.{StreamingListenerBatchSubmitted, StreamingListener}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * Spark Streaming application to benchmark performance, mainly on the driver.
 * Run with something like this:
 *  $SPARK_HOME/bin/spark-submit
 *  --class org.apache.spark.groupon.metrics.example.MetricsBenchmarkApp
 *  --master yarn-cluster
 *  --queue public
 *  --conf spark.dynamicAllocation.enabled=false
 *  --conf spark.streaming.ui.retainedBatches=120
 *  --conf spark.locality.wait=0s
 *  --files log4j.properties
 *  --num-executors 6
 *  --executor-cores 1
 *  metrics.jar 2400 12500 4
 */
object MetricsBenchmarkApp {
  def main(args: Array[String]): Unit = {
    if (args.length < 3) {
      System.err.println(
        "Usage: MetricsBenchmarkApp <timeout seconds> <metrics per second> <number of tasks that produce metrics>")
      System.exit(1)
    }
    val Seq(timeoutSeconds, metricsPerSecond, numMetricsProducers) = args.toSeq.map(_.toInt)

    val sparkConf = new SparkConf().setAppName(s"MetricsBenchmarkApp: ${metricsPerSecond * numMetricsProducers} metrics per second")
    val streamingContext = new StreamingContext(sparkConf, Seconds(1))

    UserMetricsSystem.initialize(streamingContext.sparkContext)

    streamingContext.addStreamingListener(new StreamingMetricsListener)

    val metricsStreams = for(i <- 0 until numMetricsProducers) yield {
      streamingContext.receiverStream[Long](new MetricsProducingReceiver(metricsPerSecond))
    }
    val numberStream = streamingContext.receiverStream[Long](new NumberProducingReceiver)

    streamingContext.union(metricsStreams :+ numberStream).foreachRDD(rdd => {
      lazy val actionTimer = UserMetricsSystem.timer("DriverActionTimer")
      lazy val collectCount = UserMetricsSystem.counter("BenchmarkCounterDriver")
      val isEmpty = actionTimer.time({
        rdd.isEmpty()
      })
      if (!isEmpty) collectCount.inc(numMetricsProducers)
    })

    streamingContext.start()
    streamingContext.awaitTerminationOrTimeout(timeoutSeconds * 1000)
  }
}

class StreamingMetricsListener extends StreamingListener {
  val runtime = Runtime.getRuntime
  val osStats = ManagementFactory.getOperatingSystemMXBean.asInstanceOf[com.sun.management.OperatingSystemMXBean]
  lazy val totalMemoryGauge = UserMetricsSystem.gauge("MemoryTotal")
  lazy val maxMemoryGauge = UserMetricsSystem.gauge("MemoryMax")
  lazy val freeMemoryGauge = UserMetricsSystem.gauge("MemoryFree")
  lazy val usedMemoryGauge = UserMetricsSystem.gauge("MemoryUsedPercent")
  lazy val cpuLoadGauge = UserMetricsSystem.gauge("CPULoadPercent")
  lazy val usedMemoryHistogram = UserMetricsSystem.histogram("MemoryUsedHistogram")
  lazy val cpuLoadHistogram = UserMetricsSystem.histogram("CPULoadHistogram")

  override def onBatchSubmitted(batchSubmitted: StreamingListenerBatchSubmitted): Unit = {
    val free = runtime.freeMemory()
    val total = runtime.totalMemory()
    val max = runtime.maxMemory()
    val usedMemPercent = (total - free).toFloat / total.toFloat
    val cpuLoadPercent = osStats.getProcessCpuLoad
    totalMemoryGauge.set(total / 1024)
    maxMemoryGauge.set(max / 1024)
    freeMemoryGauge.set(free / 1024)
    usedMemoryGauge.set(usedMemPercent)
    usedMemoryHistogram.update((usedMemPercent * 100).toInt)
    cpuLoadGauge.set(cpuLoadPercent)
    cpuLoadHistogram.update((cpuLoadPercent * 100).toInt)
  }

  def getSchedulerDelay(info: TaskInfo, metrics: TaskMetrics, currentTime: Long): Long = {
    val totalExecutionTime = info.finishTime - info.launchTime
    val executorOverhead = metrics.executorDeserializeTime + metrics.resultSerializationTime
    val gettingResultTime = info.finishTime - info.gettingResultTime
    math.max(0, totalExecutionTime - metrics.executorRunTime - executorOverhead - gettingResultTime)
  }
}

class MetricsProducingReceiver(val metricsPerSecond: Int) extends Receiver[Long](StorageLevel.MEMORY_ONLY) {
  lazy val counter = UserMetricsSystem.counter("BenchmarkCounter")
  lazy val loadCreatingMeter = UserMetricsSystem.meter("LoadCreatingMeter")
  @transient lazy val scheduler = new ScheduledThreadPoolExecutor(1)

  override def onStart(): Unit = {
    scheduler.scheduleAtFixedRate(
      new Runnable {
        override def run(): Unit = {
          (0 until metricsPerSecond).foreach(i => {
            loadCreatingMeter.mark()
          })
          counter.inc()
        }
      }, 0, 1, TimeUnit.SECONDS
    )
  }

  override def onStop(): Unit = {
    scheduler.shutdownNow()
  }
}

class NumberProducingReceiver extends Receiver[Long](StorageLevel.MEMORY_ONLY) {
  @transient lazy val scheduler = new ScheduledThreadPoolExecutor(1)

  override def onStart(): Unit = {
    scheduler.scheduleAtFixedRate(
      new Runnable {
        override def run(): Unit = {
          store(1)
        }
      }, 0, 1, TimeUnit.SECONDS
    )
  }

  override def onStop(): Unit = {
    scheduler.shutdownNow()
  }
}
