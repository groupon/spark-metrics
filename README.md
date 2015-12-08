#spark-metrics

A library to expose more of Spark's metrics system. This library allows you to use APIs like the [Dropwizard/Codahale Metrics](http://metrics.dropwizard.io/3.1.0/) library on Spark applications to publish metrics that are aggregated across all executors.


## Dependencies
To use this library, add a dependency to `spark-metrics` in your project:
```xml
<dependency>
    <groupId>com.groupon.dse</groupId>
    <artifactId>spark-metrics</artifactId>
    <version>0.3</version>
</dependency>
```

This library is currently built for **Spark 1.5.2**, but is also compatible with 1.4.1. This is important to note because this library uses Spark's internal APIs, and compatibility with other major Spark versions has not been fully tested.


## Usage
Include this import in your main Spark application file:
```scala
import org.apache.spark.groupon.metrics.UserMetricsSystem
```

In the Spark driver, add the following call to `UserMetricsSystem.initialize()` right after the application's `SparkContext` is instantiated:
```scala
val sparkContext = new SparkContext()
UserMetricsSystem.initialize(sparkContext, "MyMetricNamespace")
```

(Technically, it need not necessarily be *right* after the `SparkContext` is created, so long as `initialize` is called before `SparkMetric` instances are created. But invoking it here will help prevent issues related to initialization from occuring, so it is highly recommended.)

After this, you can create `SparkMetric` instances that report to Spark's metrics servlet anywhere in your application. **These instances must be declared `lazy` for this library to work properly.**
```scala
lazy val counter: SparkCounter = UserMetricsSystem.counter("MyCounter")

lazy val gauge: SparkGauge = UserMetricsSystem.gauge("MyGauge")

lazy val histogram: SparkHistogram = UserMetricsSystem.histogram("MyHistogram")

lazy val meter: SparkMeter = UserMetricsSystem.meter("MyMeter")

lazy val timer: SparkTimer = UserMetricsSystem.timer("MyTimer")
```

The `metricName` parameter is the only identifier for metrics, so different metric types cannot have the same name (e.g. a `Counter` and `Histogram` both with the same `metricName`).

The APIs for these are kept as close as possible to Dropwizard's APIs, but they don't actually extend a common interface at the language level. The only significant difference is in the `SparkGauge` class. Whereas Dropwizard's `Gauge` class is instantiated basically by passing in a function that knows how to obtain the value for the `Gauge`, this version simply has a `set()` method to set its value.


## Viewing and Publishing Metrics
This library is integrated with Spark's built-in metrics servlet. This means that metrics collected using this library are visible at the `/metrics/json/` endpoint. Here, any of the metrics from this library will be published with the key `<appName>.<metricNamespace>.<metricName>`.

These metric JSONs look something like this:
```json
application_1454970304040_0030.driver.MyAppName.MyMetricNamespace.MyTimer: {
    count: 4748,
    max: 21683.55228,
    mean: 662.7780978119895,
    min: 434.211779,
    p50: 622.795788,
    p75: 672.358402,
    p95: 1146.214833,
    p98: 1146.214833,
    p99: 1146.214833,
    p999: 1572.286154,
    stddev: 163.37016417936547,
    m15_rate: 0.06116903443100036,
    m1_rate: 0.019056182723172856,
    m5_rate: 0.051904011476711955,
    mean_rate: 0.06656686539563786,
    duration_units: "milliseconds",
    rate_units: "calls/second"
}
```

Other methods of publishing these metrics are also possible by configuring Spark. Any of the sinks listed [here](http://spark.apache.org/docs/latest/monitoring.html#metrics) will also report the metrics collected by this library as long as the `driver` instance is enabled. See [this page](https://github.com/apache/spark/blob/master/conf/metrics.properties.template) for a sample metrics configuration.


## How It Works
This library is implemented using a combination of Spark's internal RPC APIs and the Dropwizard APIs. The Dropwizard APIs are used on the driver to aggregate metrics that get collected across different executors and the driver. Spark itself uses the Dropwizard library for some of its own metrics, so this library integrates with Spark's existing metrics system to report user metrics alongside Spark's built-in metrics.

When a `SparkMetric` instance is created in an executor or the driver, it sets up a connection to the [`MetricsReceiver`](src/main/scala/org/apache/spark/groupon/metrics/MetricsReceiver.scala) on the driver, which gets set up by the call to `UserMetricsSystem.initialize`. Whenever a metric is collected (e.g. calling `meter.mark()`, `gauge.set(x)`, etc.), that value is sent to the `MetricsReceiver`, which uses those values to update its corresponding stateful Dropwizard `Metric` instance. A `SparkMetric` instance is stateless, in that there are no actual values stored there - its only functionality is to send values to the `MetricsReceiver`. A metric is uniquely identified by its name, so, for example, all values sent by instances of a `SparkHistogram` named `MyHistogram` will get aggregated on the `MetricsReceiver` in a single instance of a Dropwizard `Histogram` that corresponds to `MyHistogram`.

Metrics are sent to the `MetricsReceiver` using a [`MetricMessage`](src/main/scala/org/apache/spark/groupon/metrics/MetricMessage.scala), which contains the actual metric value and metadata about that metric. This metadata contains information that determines how its corresponding Dropwizard metric will be instantiated in the `MetricsReceiver`. For example, a `MetricMessage` for a `SparkHistogram` contains not only the metric value and name, but also the Dropwizard `Reservoir` class used to determine what kind of windowing behavior the histogram will have. Having this metadata allows for metrics to be created dynamically during runtime, rather than having to define them all beforehand. This can, for example, enable the creation of a `Meter` which is named after an `Exception`, where what the `Exception` instance could be doesn't need to be known ahead of time:
```scala
UserMetricsSystem.meter(s"exceptionRate.${exception.getClass.getSimpleName}")
```


## Troubleshooting
* A `NotInitializedException` is thrown:

  The most likely reason is that `UserMetricsSystem.initialize` was not called on the driver before a `SparkMetric` instance was created. A `SparkMetric` instance needs to connect to the `MetricsReceiver` when instantiated, so if `initialize` was not invoked, there is no `MetricsReceiver` to connect to. Another likely reason is that the `SparkMetric` instance was not declared `lazy`. This is important because, even if `initialize` was called on the driver, there's no guarantee that the `SparkMetric` instance will be instantiated on a remote JVM after the `MetricsReceiver` is set up. The only way to have this guarantee is to delay instantiating the `SparkMetric` until it is actually used by the application, which means that these need to be `lazy`. This isn't the most user-friendly API, so future work will aim to not require these `lazy` declarations.

* A `SparkContextNotFoundException` is thrown:

  This can happen if `UserMetricsSystem.initialize` is called before a `SparkContext` exists. This error can also happen if a `SparkMetric` instance isn't declared `lazy` and is instantiated as a field on the driver singleton object. A broken example:
  ```scala
  object OffsetMigrationTool {
    val myHistogram = UserMetricsSystem.histogram("MyHistogram")

    def main(args: Array[String]) {
      val sc = new SparkContext()
      UserMetricsSystem.initialize(sc)
      // Rest of driver code...
    }
  }
  ```

  `myHistogram` above needs to instead be declared `lazy`:
  ```scala
  lazy val myHistogram = UserMetricsSystem.histogram("MyHistogram")
  ```

