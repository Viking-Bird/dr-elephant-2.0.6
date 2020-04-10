/*
 * Copyright 2016 LinkedIn Corp.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.linkedin.drelephant.spark

import com.linkedin.drelephant.analysis.{HadoopAggregatedData, HadoopApplicationData, HadoopMetricsAggregator}
import com.linkedin.drelephant.configurations.aggregator.AggregatorConfigurationData
import com.linkedin.drelephant.math.Statistics
import com.linkedin.drelephant.spark.data.SparkApplicationData
import com.linkedin.drelephant.spark.heuristics.ExecutorGcHeuristic.logger
import com.linkedin.drelephant.util.MemoryFormatUtils
import org.apache.commons.io.FileUtils
import org.apache.log4j.Logger

import scala.util.Try


class SparkMetricsAggregator(private val aggregatorConfigurationData: AggregatorConfigurationData)
  extends HadoopMetricsAggregator {

  import SparkMetricsAggregator._

  private val logger: Logger = Logger.getLogger(classOf[SparkMetricsAggregator])

  private val allocatedMemoryWasteBufferPercentage: Double =
    Option(aggregatorConfigurationData.getParamMap.get(ALLOCATED_MEMORY_WASTE_BUFFER_PERCENTAGE_KEY))
      .flatMap { value => Try(value.toDouble).toOption }
      .getOrElse(DEFAULT_ALLOCATED_MEMORY_WASTE_BUFFER_PERCENTAGE)

  private val hadoopAggregatedData: HadoopAggregatedData = new HadoopAggregatedData()

  override def getResult(): HadoopAggregatedData = hadoopAggregatedData

  override def aggregate(data: HadoopApplicationData): Unit = data match {
    case (data: SparkApplicationData) => aggregate(data)
    case _ => throw new IllegalArgumentException("data should be SparkApplicationData")
  }

  private def aggregate(data: SparkApplicationData): Unit = for {
    executorInstances <- executorInstancesOf(data)
    executorMemoryBytes <- executorMemoryBytesOf(data)
  } {
    val applicationDurationMillis = applicationDurationMillisOf(data)
    if (applicationDurationMillis < 0) {
      logger.warn(s"applicationDurationMillis is negative. Skipping Metrics Aggregation:${applicationDurationMillis}")
    } else {
      // 获取所有Executor的执行时间
      val totalExecutorTaskTimeMillis = totalExecutorTaskTimeMillisOf(data)

      // 计算分配给任务的总资源，以MB/S表示
      val resourcesAllocatedForUse =
        aggregateresourcesAllocatedForUse(executorInstances, executorMemoryBytes, applicationDurationMillis)
      // 计算任务实际使用的总资源
      val resourcesActuallyUsed = aggregateresourcesActuallyUsed(executorMemoryBytes, totalExecutorTaskTimeMillis)
      // 计算任务实际使用和缓存的总资源
      val resourcesActuallyUsedWithBuffer = resourcesActuallyUsed.doubleValue() * (1.0 + allocatedMemoryWasteBufferPercentage)
      // 如果任务实际使用和缓存的总资源小于给任务分配的总资源的话（大于任务会被Kill掉），就用resourcesAllocatedForUse.doubleValue() - resourcesActuallyUsedWithBuffer计算任务浪费的资源
      val resourcesWastedMBSeconds = (resourcesActuallyUsedWithBuffer < resourcesAllocatedForUse.doubleValue()) match {
        case true => resourcesAllocatedForUse.doubleValue() - resourcesActuallyUsedWithBuffer
        case false => 0.0
      }
      //allocated is the total used resource from the cluster.
      if (resourcesAllocatedForUse.isValidLong) {
        hadoopAggregatedData.setResourceUsed(resourcesAllocatedForUse.toLong)
      } else {
        logger.warn(s"resourcesAllocatedForUse/resourcesWasted exceeds Long.MaxValue")
        logger.warn(s"ResourceUsed: ${resourcesAllocatedForUse}")
        logger.warn(s"executorInstances: ${executorInstances}")
        logger.warn(s"executorMemoryBytes:${executorMemoryBytes}")
        logger.warn(s"applicationDurationMillis:${applicationDurationMillis}")
        logger.warn(s"totalExecutorTaskTimeMillis:${totalExecutorTaskTimeMillis}")
        logger.warn(s"resourcesActuallyUsedWithBuffer:${resourcesActuallyUsedWithBuffer}")
        logger.warn(s"resourcesWastedMBSeconds:${resourcesWastedMBSeconds}")
        logger.warn(s"allocatedMemoryWasteBufferPercentage:${allocatedMemoryWasteBufferPercentage}")
      }
      hadoopAggregatedData.setResourceWasted(resourcesWastedMBSeconds.toLong)
    }
  }

  private def aggregateresourcesActuallyUsed(executorMemoryBytes: Long, totalExecutorTaskTimeMillis: BigInt): BigInt = {
    val bytesMillis = BigInt(executorMemoryBytes) * totalExecutorTaskTimeMillis
    (bytesMillis / (BigInt(FileUtils.ONE_MB) * BigInt(Statistics.SECOND_IN_MS)))
  }

  private def aggregateresourcesAllocatedForUse(
                                                 executorInstances: Int,
                                                 executorMemoryBytes: Long,
                                                 applicationDurationMillis: Long
                                               ): BigInt = {
    val bytesMillis = BigInt(executorInstances) * BigInt(executorMemoryBytes) * BigInt(applicationDurationMillis)
    (bytesMillis / (BigInt(FileUtils.ONE_MB) * BigInt(Statistics.SECOND_IN_MS)))
  }

  private def executorInstancesOf(data: SparkApplicationData): Option[Int] = {
    val appConfigurationProperties = data.appConfigurationProperties
    val isDynamicAllocationEnabled = Some(appConfigurationProperties.get(SPARK_DYNAMIC_ALLOCATION_ENABLED).exists(_.toBoolean == true))
    isDynamicAllocationEnabled match {
      case Some(true) => Some(data.executorSummaries.size)
      case Some(false) => appConfigurationProperties.get(SPARK_EXECUTOR_INSTANCES_KEY).map(_.toInt)
    }
  }

  private def executorMemoryBytesOf(data: SparkApplicationData): Option[Long] = {
    val appConfigurationProperties = data.appConfigurationProperties
    appConfigurationProperties.get(SPARK_EXECUTOR_MEMORY_KEY).map(MemoryFormatUtils.stringToBytes)
  }

  private def applicationDurationMillisOf(data: SparkApplicationData): Long = {
    require(data.applicationInfo.attempts.nonEmpty)
    val lastApplicationAttemptInfo = data.applicationInfo.attempts.last
    logger.info(s"Spark ${data.getAppId} attempts.size: ${data.applicationInfo.attempts.size}, last.attemptId: ${lastApplicationAttemptInfo.attemptId}, last.attempt.endTime: ${lastApplicationAttemptInfo.endTime.getTime}, last.attempt.startTime: ${lastApplicationAttemptInfo.startTime.getTime}")
    lastApplicationAttemptInfo.endTime.getTime - lastApplicationAttemptInfo.startTime.getTime
  }

  private def totalExecutorTaskTimeMillisOf(data: SparkApplicationData): BigInt = {
    data.executorSummaries.map { executorSummary => BigInt(executorSummary.totalDuration) }.sum
  }
}

object SparkMetricsAggregator {
  /** The percentage of allocated memory we expect to waste because of overhead. */
  val DEFAULT_ALLOCATED_MEMORY_WASTE_BUFFER_PERCENTAGE = 0.5D

  val ALLOCATED_MEMORY_WASTE_BUFFER_PERCENTAGE_KEY = "allocated_memory_waste_buffer_percentage"

  val SPARK_EXECUTOR_INSTANCES_KEY = "spark.executor.instances"
  val SPARK_EXECUTOR_MEMORY_KEY = "spark.executor.memory"
  val SPARK_DYNAMIC_ALLOCATION_ENABLED = "spark.dynamicAllocation.enabled"
}
