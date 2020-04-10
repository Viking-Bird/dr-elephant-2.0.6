package org.apache.spark.deploy.history

import java.io.{File, FileInputStream, InputStream}

import com.linkedin.drelephant.spark.fetchers.statusapiv1.ExecutorSummary
import com.linkedin.drelephant.spark.legacydata.LegacyDataConverters
import org.apache.spark.SparkConf
import org.apache.spark.io.LZ4BlockInputStream
import org.apache.spark.scheduler.{ApplicationEventListener, ReplayListenerBus}
import org.apache.spark.storage.{StorageStatusListener, StorageStatusTrackingListener}
import org.apache.spark.ui.env.EnvironmentListener
import org.apache.spark.ui.exec.ExecutorsListener
import org.apache.spark.ui.jobs.JobProgressListener
import org.apache.spark.ui.storage.StorageListener
import org.xerial.snappy.SnappyInputStream

/**
  *
  *
  * @author pengwang
  * @date 2020/04/10
  */
object SparkDataCollectionTest2 {

  lazy val applicationEventListener = new ApplicationEventListener()
  lazy val jobProgressListener = new JobProgressListener(new SparkConf())
  lazy val environmentListener = new EnvironmentListener()
  lazy val storageStatusListener = new StorageStatusListener(new SparkConf())
  lazy val executorsListener = new ExecutorsListener(storageStatusListener, new SparkConf())
  lazy val storageListener = new StorageListener(storageStatusListener)

  // This is a customized listener that tracks peak used memory
  // The original listener only tracks the current in use memory which is useless in offline scenario.
  lazy val storageStatusTrackingListener = new StorageStatusTrackingListener()

  def parseSnappyFile(path: String, suffix: String) = {
    var in: InputStream = null
    if (suffix.eq("lz4")) {
      in = new LZ4BlockInputStream(new FileInputStream(new File(path)))
    } else {
      in = new SnappyInputStream(new FileInputStream(new File(path)))
    }

    val dataCollection = new SparkDataCollection()
    dataCollection.load(in, path)
    LegacyDataConverters.convert(dataCollection)
  }

  def main(args: Array[String]): Unit = {
    val data = parseSnappyFile("/Users/pengwang/Documents/TestProject/sz/application_1579075466726_1164448_1.lz4", "lz4")
    println(getTimeValues(data.executorSummaries))

    val (jvmTime, executorRunTimeTotal) = getTimeValues(data.executorSummaries)
    val ratio: Double = jvmTime.toDouble / executorRunTimeTotal.toDouble
    println(ratio)
  }

  /**
    * returns the total JVM GC Time and total executor Run Time across all stages
    *
    * @param executorSummaries
    * @return
    */
  private def getTimeValues(executorSummaries: Seq[ExecutorSummary]): (Long, Long) = {
    var jvmGcTimeTotal: Long = 0
    var executorRunTimeTotal: Long = 0
    executorSummaries.foreach(executorSummary => {
      jvmGcTimeTotal += executorSummary.totalGCTime
      executorRunTimeTotal += executorSummary.totalDuration
    })
    (jvmGcTimeTotal, executorRunTimeTotal)
  }
}