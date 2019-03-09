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

import java.net.URI
import java.util
import java.util.Properties
import java.util.concurrent.TimeUnit

import com.banzaicloud.metrics.prometheus.client.exporter.PushGatewayWithTimestamp
import com.codahale.metrics._
import io.prometheus.client.{Collector, CollectorRegistry}
import org.apache.spark.internal.Logging
import org.apache.spark.metrics.sink.Sink
import org.apache.spark.{SecurityManager, SparkConf, SparkEnv}

import scala.collection.JavaConverters._
import scala.util.Try


class PrometheusSink(
                      val property: Properties,
                      val registry: MetricRegistry,
                      securityMgr: SecurityManager)
  extends Sink with Logging {

  protected class PrometheusJobConfig {
    val defaultSparkConf: SparkConf = new SparkConf(true)
    logInfo("Initializing JobConfig")

    // SparkEnv may become available only after metrics sink creation thus retrieving
    // SparkConf from spark env here and not during the creation/initialisation of PrometheusSink.
    val sparkConf: SparkConf = Option(SparkEnv.get).map(_.conf).getOrElse(defaultSparkConf)

    val metricsNamespace: Option[String] = sparkConf.getOption("spark.metrics.namespace")
    val sparkAppId: String = sparkConf.getOption("spark.app.id").get
    val sparkAppName: String = sparkConf.getOption("spark.app.name").getOrElse("")
    val executorId: Option[String] = sparkConf.getOption("spark.executor.id")

    val role = executorId match {
      case Some("driver") | Some("<driver>") => "driver"
      case Some(_) => "executor"
      case _ => "shuffle"
    }

    val job = role match {
      case "driver" => metricsNamespace.getOrElse(sparkAppId)
      case "executor" => metricsNamespace.getOrElse(sparkAppId)
      case _ => metricsNamespace.getOrElse("shuffle")
    }

    val groupingKey = {
      (role, executorId) match {
        case ("driver", _) => Map("role" -> role)
        case ("executor", Some(id)) => Map("role" -> role, "number" -> id)
        case _ => Map("role" -> role)
      }
    }.asJava

    logInfo(s"metricsNamespace=$metricsNamespace, sparkAppId=$sparkAppId, " +
      s"sparkAppName=$sparkAppName, role=$role, executorId=$executorId")
  }

  protected class Reporter(registry: MetricRegistry)
    extends ScheduledReporter(
      registry,
      "prometheus-reporter",
      MetricFilter.ALL,
      TimeUnit.SECONDS,
      TimeUnit.MILLISECONDS) {

    lazy val conf = new PrometheusJobConfig
    lazy val sparkMetricExports: Collector = new SparkMetricExports(conf.metricsNamespace.getOrElse(conf.sparkAppId),conf.sparkAppName,registry)

    override def report(
                         gauges: util.SortedMap[String, Gauge[_]],
                         counters: util.SortedMap[String, Counter],
                         histograms: util.SortedMap[String, Histogram],
                         meters: util.SortedMap[String, Meter],
                         timers: util.SortedMap[String, Timer]): Unit = {
      logInfo("Reporting Metrics")

      val metricTimestamp = if (enableTimestamp) Some(s"${System.currentTimeMillis}") else None

      pushGateway.pushAdd(sparkMetricExports, conf.job, conf.groupingKey, metricTimestamp.orNull)
    }
  }

  val DEFAULT_PUSH_PERIOD: Int = 10
  val DEFAULT_PUSH_PERIOD_UNIT: TimeUnit = TimeUnit.SECONDS
  val DEFAULT_PUSHGATEWAY_ADDRESS: String = "127.0.0.1:9091"
  val DEFAULT_PUSHGATEWAY_ADDRESS_PROTOCOL: String = "http"
  val PUSHGATEWAY_ENABLE_TIMESTAMP: Boolean = false

  val KEY_PUSH_PERIOD = "period"
  val KEY_PUSH_PERIOD_UNIT = "unit"
  val KEY_PUSHGATEWAY_ADDRESS = "pushgateway-address"
  val KEY_PUSHGATEWAY_ADDRESS_PROTOCOL = "pushgateway-address-protocol"
  val KEY_PUSHGATEWAY_ENABLE_TIMESTAMP = "pushgateway-enable-timestamp"


  val pollPeriod: Int =
    Option(property.getProperty(KEY_PUSH_PERIOD))
      .map(_.toInt)
      .getOrElse(DEFAULT_PUSH_PERIOD)

  val pollUnit: TimeUnit =
    Option(property.getProperty(KEY_PUSH_PERIOD_UNIT))
      .map { s => TimeUnit.valueOf(s.toUpperCase) }
      .getOrElse(DEFAULT_PUSH_PERIOD_UNIT)

  val pushGatewayAddress =
    Option(property.getProperty(KEY_PUSHGATEWAY_ADDRESS))
      .getOrElse(DEFAULT_PUSHGATEWAY_ADDRESS)

  val pushGatewayAddressProtocol =
    Option(property.getProperty(KEY_PUSHGATEWAY_ADDRESS_PROTOCOL))
      .getOrElse(DEFAULT_PUSHGATEWAY_ADDRESS_PROTOCOL)

  val enableTimestamp: Boolean =
    Option(property.getProperty(KEY_PUSHGATEWAY_ENABLE_TIMESTAMP))
      .map(_.toBoolean)
      .getOrElse(PUSHGATEWAY_ENABLE_TIMESTAMP)

  // validate pushgateway host:port
  Try(new URI(s"$pushGatewayAddressProtocol://$pushGatewayAddress")).get

  checkMinimalPollingPeriod(pollUnit, pollPeriod)

  logInfo("Initializing Prometheus Sink...")
  logInfo(s"Metrics polling period -> $pollPeriod $pollUnit")
  logInfo(s"Metrics timestamp enabled -> $enableTimestamp")
  logInfo(s"$KEY_PUSHGATEWAY_ADDRESS -> $pushGatewayAddress")
  logInfo(s"$KEY_PUSHGATEWAY_ADDRESS_PROTOCOL -> $pushGatewayAddressProtocol")

  val pushRegistry: CollectorRegistry = new CollectorRegistry()
  val pushGateway: PushGatewayWithTimestamp =
    new PushGatewayWithTimestamp(s"$pushGatewayAddressProtocol://$pushGatewayAddress")

  val reporter = new Reporter(registry)

  override def start(): Unit = {
    reporter.start(pollPeriod, pollUnit)
  }

  override def stop(): Unit = {
    reporter.stop()
    pushRegistry.clear()
  }

  override def report(): Unit = {
    reporter.report()
  }

  private def checkMinimalPollingPeriod(pollUnit: TimeUnit, pollPeriod: Int) {
    val period = TimeUnit.SECONDS.convert(pollPeriod, pollUnit)
    if (period < 1) {
      throw new IllegalArgumentException("Polling period " + pollPeriod + " " + pollUnit +
        " below than minimal polling period ")
    }
  }
}
