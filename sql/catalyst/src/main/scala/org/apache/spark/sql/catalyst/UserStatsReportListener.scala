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

package org.apache.spark.sql.catalyst

import org.apache.spark.JobExecutionStatus
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.internal.Logging
import org.apache.spark.scheduler._

import scala.collection.mutable


@DeveloperApi
class UserStatsReportListener extends SparkListener with Logging {

  private val _jobIdToExecutionId = mutable.HashMap[Long, Long]()

  private val _stageIdToStageMetrics = mutable.HashMap[Long, StageMetrics]()

  private val activeStages = mutable.HashMap[]
  private val _executionIdToData = mutable.HashMap[Long, ExecutionData]()
  private val activeExecutions = mutable.HashMap[Long, ExecutionData]()

  override def onStageSubmitted(stageSubmitted: SparkListenerStageSubmitted): Unit = synchronized {
    val stageId = stageSubmitted.stageInfo.stageId
    val stageAttemptId = stageSubmitted.stageInfo.attemptId
    _stageIdToStageMetrics(stageId) = new StageMetrics(stageAttemptId)

    // Always override metrics for old stage attempt
    // if (_stageIdToStageMetrics.contains(stageId)) {
    //   _stageIdToStageMetrics(stageId) = new StageMetrics(stageAttemptId)
    // } else {
      // If a stage belongs to some SQL execution, its stageId will be put in "onJobStart".
      // Since "_stageIdToStageMetrics" doesn't contain it, it must not belong to any SQL execution.
      // So we can ignore it. Otherwise, this may lead to memory leaks (SPARK-11126).
    // }
  }

  override def onTaskEnd(taskEnd: SparkListenerTaskEnd) {
    if (taskEnd.taskMetrics != null) {
      // scalastyle:off println
      taskEnd.taskMetrics.externalAccums.foreach(x =>
        logWarning("onTaskEnd :: " + x.metadata.name) )
      // scalastyle:on
      updateTaskAccumulatorValues(
        taskEnd.taskInfo.taskId,
        taskEnd.stageId,
        taskEnd.stageAttemptId,
        taskEnd.taskMetrics.externalAccums.map(a => a.toInfo(Some(a.value), None)),
        finishTask = true)
    }
  }

  /**
    * Update the accumulator values of a task with the latest metrics for this task. This is called
    * every time we receive an executor heartbeat or when a task finishes.
    */
  protected def updateTaskAccumulatorValues(
               taskId: Long,
               stageId: Int,
               stageAttemptID: Int,
               _accumulatorUpdates: Seq[AccumulableInfo],
               finishTask: Boolean): Unit = {


    val accumulatorUpdates =
      _accumulatorUpdates.filter(_.update.isDefined).map(accum => {
        (accum.id, accum.update.get, accum.name match {
          case Some(name) => name
          case None => ""
        })
      })

    accumulatorUpdates.foreach(x => logWarning("updateTaskAccum " + x._3))

    _stageIdToStageMetrics.get(stageId) match {
      case Some(stageMetrics) =>
        if (stageAttemptID < stageMetrics.stageAttemptId) {
          // A task of an old stage attempt. Because a new stage is submitted, we can ignore it.
        } else if (stageAttemptID > stageMetrics.stageAttemptId) {
          logWarning(s"A task should not have a higher stageAttemptID ($stageAttemptID) then " +
            s"what we have seen (${stageMetrics.stageAttemptId})")
        } else {
          // TODO We don't know the attemptId. Currently, what we can do is overriding the
          // accumulator updates. However, if there are two same task are running, such as
          // speculation, the accumulator updates will be overriding by different task attempts,
          // the results will be weird.
          stageMetrics.taskIdToMetricUpdates.get(taskId) match {
            case Some(taskMetrics) =>
              if (finishTask) {
                taskMetrics.finished = true
                taskMetrics.accumulatorUpdates = accumulatorUpdates
              } else if (!taskMetrics.finished) {
                taskMetrics.accumulatorUpdates = accumulatorUpdates
              } else {
                // If a task is finished, we should not override with accumulator updates from
                // heartbeat reports
              }
            case None =>
              // TODO Now just set attemptId to 0. Should fix here when we can get the attempt
              // id from SparkListenerExecutorMetricsUpdate
              stageMetrics.taskIdToMetricUpdates(taskId) = new UserTaskMetrics(
                attemptId = 0, finished = finishTask, accumulatorUpdates)
          }
        }
      case None =>
      // This execution and its stage have been dropped
    }
  }

  override def onJobStart(jobStart: SparkListenerJobStart): Unit = {
    val executionIdString = jobStart.properties.getProperty(SQLExecution.EXECUTION_ID_KEY)
    if (executionIdString == null) {
      // This is not a job created by SQL
      return
    }
    val executionId = executionIdString.toLong
    val jobId = jobStart.jobId
    val stageIds = jobStart.stageIds

    synchronized {
      activeExecutions.get(executionId).foreach { executionData =>
        // executionData.jobs(jobId) = JobExecutionStatus.RUNNING
        executionData.stages ++= stageIds
        stageIds.foreach(stageId =>
          _stageIdToStageMetrics(stageId) = new StageMetrics(stageAttemptId = 0))
        _jobIdToExecutionId(jobId) = executionId
      }
    }
  }

  override def onJobEnd(jobEnd: SparkListenerJobEnd): Unit = synchronized {
    val jobId = jobEnd.jobId
    for (executionId <- _jobIdToExecutionId.get(jobId);
         executionUIData <- _executionIdToData.get(executionId)) {
        markExecutionFinished(executionId)
    }
  }

  private def markExecutionFinished(executionId: Long): Unit = {
    activeExecutions.remove(executionId)
  }

  def getExecutionMetrics(executionId: Long): Map[Long, String] = synchronized {
    _executionIdToData.get(executionId) match {
      case Some(executionUIData) =>
        val accumulatorUpdates = {
          for (stageId <- executionUIData.stages;
               stageMetrics <- _stageIdToStageMetrics.get(stageId).toIterable;
               taskMetrics <- stageMetrics.taskIdToMetricUpdates.values;
               accumulatorUpdate <- taskMetrics.accumulatorUpdates) yield {
            logWarning("getExecMetrics " + accumulatorUpdate._3)
            (accumulatorUpdate._1, (accumulatorUpdate._3 + ":" + accumulatorUpdate._2).toString)
          }
        }
        // }.filter { case (id, _) => executionUIData.accumulatorMetrics.contains(id) }

        // val driverUpdates = executionUIData.driverAccumUpdates.toSeq
        mergeAccumulatorUpdates(accumulatorUpdates.)
      case None =>
        // This execution has been dropped
        Map.empty
    }
  }

  private def mergeAccumulatorUpdates(
       accumulatorUpdates: Seq[(Long, Any)]): Map[Long, String] = {

    accumulatorUpdates.groupBy(_._1).map { case (accumulatorId, values) =>
      // val metricType = metricTypeFunc(accumulatorId)
      accumulatorId ->
        UserTaskMetrics.stringValue(values.map(_._2))
      // SQLMetrics.stringValue(metricType, values.map(_._2))
    }
  }

  override def onStageCompleted(stageCompleted: SparkListenerStageCompleted) {
    this.logInfo(s"Finished stage: ${getStatusDetail(stageCompleted.stageInfo)}")
    if (! _executionIdToData.isEmpty) {
      val latestExecID = _executionIdToData.last._1
      // scalastyle:off println
      println("Executor ID " + latestExecID)
      getExecutionMetrics(latestExecID).foreach(x => this.logInfo(x._1 + " : " + x._2))
      // scalastyle:on println
    }
  }

  private def getStatusDetail(info: StageInfo): String = {
    val failureReason = info.failureReason.map("(" + _ + ")").getOrElse("")
    val timeTaken = info.submissionTime.map(
      x => info.completionTime.getOrElse(System.currentTimeMillis()) - x
    ).getOrElse("-")

    s"Stage(${info.stageId}, ${info.attemptId}); Name: '${info.name}'; " +
      s"Status: ${info.getStatusString}$failureReason; numTasks: ${info.numTasks}; " +
      s"Took: $timeTaken msec"
  }

}

object SQLStatsReportListener {

}

private case class PlanMetric(
    name: String,
    accumulatorId: Long,
    metricType: String)

private class StageMetrics(
     val stageAttemptId: Long,
     val taskIdToMetricUpdates: mutable.HashMap[Long, UserTaskMetrics] = mutable.HashMap.empty)

private class UserTaskMetrics(
      val attemptId: Long, // TODO not used yet
      var finished: Boolean,
      var accumulatorUpdates: Seq[(Long, Any, String)])

private class ExecutionData( 
  val executionId: Long, 
  val accumulatorMetrics: Map[Long, UserTaskMetrics]) {  
    val stages: mutable.ArrayBuffer[Int] = mutable.ArrayBuffer()
}
