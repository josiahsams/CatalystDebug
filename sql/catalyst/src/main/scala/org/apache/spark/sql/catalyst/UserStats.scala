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

import scala.collection.mutable
import org.apache.spark.executor.TaskMetrics
import org.apache.spark.internal.Logging
import org.apache.spark.scheduler._

class UserStats extends SparkListener with Logging {

  private val taskInfoMetrics = mutable.Buffer[(TaskInfo, TaskMetrics)]()

  override def onTaskEnd(taskEnd: SparkListenerTaskEnd) {
    val info = taskEnd.taskInfo
    val metrics = taskEnd.taskMetrics
    if (info != null && metrics != null) {
      taskInfoMetrics += ((info, metrics))
    }
  }

  override def onStageCompleted(stageCompleted: SparkListenerStageCompleted) {
    implicit val sc = stageCompleted
    this.logInfo(s"Finished stage: ${getStatusDetail(stageCompleted.stageInfo)}")

    val accumulatorUpdates = mutable.Buffer[(Long, Any)]()

    taskInfoMetrics.map{ case(_, metrics) =>
      metrics.internalAccums.filter(
        ac =>
          if (ac.metadata != null) {
            ac.metadata.name.getOrElse("").contains("userdefined")
          } else false ).map(
        a => a.toInfo(Some(a.value), None)).filter(_.update.isDefined)
        .map(accum => {
          accumulatorUpdates +=
            ((accum.id, accum.name.getOrElse("") + ":" + accum.update.get.toString))
        })

      metrics.externalAccums.map(a =>
        a.toInfo(Some(a.value), None)).filter(_.update.isDefined)
        .map(accum => {
          accumulatorUpdates +=
            ((accum.id, accum.name.getOrElse("") + ":" + accum.update.get.toString))
        })

    }

    accumulatorUpdates.groupBy(_._1).map { case (accumulatorId, values) =>
      accumulatorId ->
        UserTaskMetrics.stringValue(values.map(_._2))
    }.foreach(x => this.logError(x._1 + " : " + x._2))

    taskInfoMetrics.clear()
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
