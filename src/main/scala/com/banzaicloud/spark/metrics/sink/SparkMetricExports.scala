/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.banzaicloud.spark.metrics.sink

import java.util.concurrent.TimeUnit
import java.util.regex.Pattern
import java.util.{ArrayList, Collections, List}

import com.codahale.metrics._
import io.prometheus.client.Collector.{MetricFamilySamples, Type}
import org.apache.spark.internal.Logging


/**
  * A replacement for the dropwizards exports that reduces the number of prometheus metrics names.
  *
  * Typical original patterns produced by spark are:
  * {namespace}.driver.{metricName}
  * {namespace}.{executorId}.executor.{metricName}
  * {namespace}.driver.{appname}.StreamingMetrics.streaming.{metricName}
  *
  * The pattern for {metricName} is:
  *  {word}[.{word}]+
  *
  *  This exporter aims to produce prometheus metrics where:
  *  1) the {metricName} is the name of the metric
  *  2) {namespace} is the job label
  *  3) executor or driver populates the 'role' label
  *  4) an optional 'number' label is created from the executorId
  *
  *  Since the namespace, role and executorId can be extracted from the spark context, we
  *  only care about extracting the metricName in this class.
  *
  * @param namespace
  * @param appname
  * @param registry
  */
class SparkMetricExports(
                          val namespace:String,
                          val appname:String,
                          registry: MetricRegistry
                        ) extends io.prometheus.client.Collector with io.prometheus.client.Collector.Describable with Logging {

  val METRIC_NAME_RE = Pattern.compile("[^a-zA-Z0-9:_]")
  val EMPTY_LIST: java.util.List[String] = java.util.Collections.emptyList()
  val METRIC_NAME_TEMPLATE = "%s\\.((\\d+)|driver)\\.(.+)"
  val STREAMING_METRIC_NAME_TEMPLATE = "%s\\.driver\\.%s\\.StreamingMetrics\\.(streaming\\..+)"
  val pattern = Pattern.compile(String.format(METRIC_NAME_TEMPLATE, namespace))
  val streamingPattern = Pattern.compile(String.format(STREAMING_METRIC_NAME_TEMPLATE, namespace, appname))



  override def collect(): List[MetricFamilySamples] = {

    val mfSamples = new ArrayList[MetricFamilySamples]

    import scala.collection.JavaConversions._
    registry.getGauges.foreach(kv => mfSamples.add(fromGauge(kv._1, kv._2)))
    registry.getCounters.foreach(kv => mfSamples.add(fromCounter(kv._1, kv._2)))
    registry.getHistograms.foreach(kv => mfSamples.add(fromHistogram(kv._1, kv._2)))
    registry.getTimers.foreach(kv => mfSamples.add(fromTimer(kv._1, kv._2)))
    registry.getMeters.foreach(kv => mfSamples.add(fromMeter(kv._1, kv._2)))
    mfSamples
  }

  override def describe(): List[MetricFamilySamples] = {
    java.util.Collections.emptyList()
  }

  def extractMetricName(metricName: String): String = {

    val matcher = streamingPattern.matcher(metricName)
    if (matcher.find()) {
      matcher.group(1)
    }
    else {
      val matcher = pattern.matcher(metricName)
      if (matcher.find()) {
        matcher.group(3)
      }
      else {
        ""
      }
    }
  }

  def sanitizeMetricName(origName: String): String = {

    val name = "spark_"+METRIC_NAME_RE.matcher(extractMetricName(origName)).replaceAll("_")
    logDebug(s"$origName changed to $name")
    name
  }

  def fromCounter(origMetricName: String, counter: Counter): MetricFamilySamples = {
    val name: String = sanitizeMetricName(origMetricName)
    val sample = new MetricFamilySamples.Sample(name, EMPTY_LIST, EMPTY_LIST, counter.getCount.doubleValue())
    new MetricFamilySamples(name, Type.GAUGE, getHelpMessage(origMetricName, counter), Collections.singletonList(sample))
  }

  /**
    * Export gauge as a prometheus gauge.
    */
  def fromGauge(origMetricName: String, gauge: Gauge[_]): MetricFamilySamples = {
    val name = sanitizeMetricName(origMetricName)
    val value: Double = gauge.getValue match {
      case n: Number => n.doubleValue()
      case b: Boolean => if (b) 1 else 0
      case _ => null.asInstanceOf[Double]
    }
    val sample = new MetricFamilySamples.Sample(name, EMPTY_LIST, EMPTY_LIST, value)
    new MetricFamilySamples(name, Type.GAUGE, getHelpMessage(origMetricName, gauge), Collections.singletonList(sample))

  }

  /**
    * Export counter as Prometheus <a href="https://prometheus.io/docs/concepts/metric_types/#gauge">Gauge</a>.
    */

  /**
    * Convert histogram snapshot.
    */
  def fromHistogram(origMetricName: String, histogram: Histogram): MetricFamilySamples = {
    fromSnapshotAndCount(origMetricName, histogram.getSnapshot(), histogram.getCount(), 1.0, getHelpMessage(origMetricName, histogram))
  }

  /**
    * Export Dropwizard Timer as a histogram. Use TIME_UNIT as time unit.
    */
  def fromTimer(origMetricName: String, timer: Timer): MetricFamilySamples = {
    fromSnapshotAndCount(origMetricName, timer.getSnapshot(), timer.getCount(),
      1.0D / TimeUnit.SECONDS.toNanos(1L), getHelpMessage(origMetricName, timer))
  }

  /**
    * Export a Meter as as prometheus COUNTER.
    */
  def fromMeter(origMetricName: String, meter: Meter): MetricFamilySamples = {
    val name = sanitizeMetricName(origMetricName);
    val sample = new MetricFamilySamples.Sample(name + "_total", EMPTY_LIST, EMPTY_LIST, meter.getCount)
    new MetricFamilySamples(name + "_total", Type.COUNTER, getHelpMessage(origMetricName, meter), Collections.singletonList(sample))
  }


  /**
    * Export a histogram snapshot as a prometheus SUMMARY.
    *
    * @param origName metric name.
    * @param snapshot the histogram snapshot.
    * @param count    the total sample count for this snapshot.
    * @param factor   a factor to apply to histogram values.
    *
    */
  def fromSnapshotAndCount(origName: String, snapshot: Snapshot, count: Long, factor: Double, helpMessage: String): MetricFamilySamples = {
    val name = sanitizeMetricName(origName)
    val samples: java.util.List[MetricFamilySamples.Sample] = java.util.Arrays.asList(
      new MetricFamilySamples.Sample(name, Collections.singletonList("quantile"), Collections.singletonList("0.5"), snapshot.getMedian() * factor),
      new MetricFamilySamples.Sample(name, Collections.singletonList("quantile"), Collections.singletonList("0.75"), snapshot.get75thPercentile() * factor),
      new MetricFamilySamples.Sample(name, Collections.singletonList("quantile"), Collections.singletonList("0.95"), snapshot.get95thPercentile() * factor),
      new MetricFamilySamples.Sample(name, Collections.singletonList("quantile"), Collections.singletonList("0.98"), snapshot.get98thPercentile() * factor),
      new MetricFamilySamples.Sample(name, Collections.singletonList("quantile"), Collections.singletonList("0.99"), snapshot.get99thPercentile() * factor),
      new MetricFamilySamples.Sample(name, Collections.singletonList("quantile"), Collections.singletonList("0.999"), snapshot.get999thPercentile() * factor),
      new MetricFamilySamples.Sample(name + "_count", EMPTY_LIST, EMPTY_LIST, count)
    )
    new MetricFamilySamples(name, Type.SUMMARY, helpMessage, samples)

  }


  def getHelpMessage(metricName: String, metric: Metric): String = {
    return String.format("Generated from SparkMetricsExports metric import (metric=%s, type=%s)",
      metricName, metric.getClass().getName());
  }


}





