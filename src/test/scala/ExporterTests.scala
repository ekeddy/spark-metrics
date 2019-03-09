import com.banzaicloud.spark.metrics.sink.SparkMetricExports
import com.codahale.metrics.{Counter, Gauge}
import org.scalatest.{FlatSpec, Matchers}

class ExporterTests  extends FlatSpec with Matchers {


  "A SparkMetricsExporter" should "sanitize metric name" in {

    val exporter = new SparkMetricExports("metricsstreaming","MetricsStreaming",null)

    var res = exporter.sanitizeMetricName("hi")
    res shouldBe "spark_"

    res = exporter.sanitizeMetricName("spark_metricsstreaming.1.abc")
    res shouldBe "spark_abc"

    res = exporter.sanitizeMetricName("spark_metricsstreaming.10.abc")
    res shouldBe "spark_abc"

    res = exporter.sanitizeMetricName("spark_metricsstreaming.110.abc")
    res shouldBe "spark_abc"

    res = exporter.sanitizeMetricName("spark_metricsstreaming.driver.MetricsStreaming.StreamingMetrics.streaming.lastCompletedBatch_processingStartTime")
    res shouldBe "spark_streaming_lastCompletedBatch_processingStartTime"

  }

  it should "convert a counter" in {
    val exporter = new SparkMetricExports("metricsstreaming","bob",null)

    var res = exporter.fromCounter("spark_metricsstreaming.110.abc",new Counter)
    res.name shouldBe "spark_abc"
    res.samples.get(0).value shouldBe 0.0d

    //println(res)
  }

  it should "convert a gauge" in {
    val exporter = new SparkMetricExports("metricsstreaming","bob",null)
    var res = exporter.fromGauge("spark_metricsstreaming.110.abc",new Gauge[Integer]() {
      def getValue:Integer = 3 })
    res.name shouldBe "spark_abc"
    res.samples.get(0).value shouldBe 3.0d

    //println(res)
  }

}
