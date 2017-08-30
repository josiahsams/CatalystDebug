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

import java.text.NumberFormat

import org.apache.spark.executor.TaskMetrics
import org.apache.spark.scheduler.AccumulableInfo
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenContext
import org.apache.spark.util.{AccumulatorContext, AccumulatorV2, LongAccumulator}
import org.apache.spark.{SparkContext, SparkEnv, TaskContext}

import scala.collection.mutable.ArrayBuffer

class UserTaskMetric(initValue: Long = 0L)
  extends AccumulatorV2[Long, Long] {
  private[this] var _value = initValue
  private var _zeroValue = initValue

  override def copy(): UserTaskMetric = {
    val newAcc = new UserTaskMetric(_value)
    newAcc._zeroValue = initValue
    newAcc
  }

  override def reset(): Unit = _value = {
    // scalastyle:off println
    if (this.metadata != null ) {

      println("Reset on Object " + this + " ID " + this.metadata.id)
    } else {
      println("Reset on Object " + this)
    }
    // scalastyle:on
    _zeroValue
  }

  override def merge(other: AccumulatorV2[Long, Long]): Unit = other match {
    case o: UserTaskMetric => _value += o.value
    case _ => throw new UnsupportedOperationException(
      s"Cannot merge ${this.getClass.getName} with ${other.getClass.getName}")
  }

  override def isZero(): Boolean = _value == _zeroValue

  override def add(v: Long): Unit = {
    // scalastyle:off println
    println("Add on Object " + this + " ID " + this.metadata.id)
    // scalastyle:on
    _value += v
  }

  def +=(v: Long): Unit = _value += v

  override def value: Long = _value

  def showMetricName: String = metadata.name match {
    case Some(name) => name
    case None => ""
  }

  // Provide special identifier as metadata so we can tell that this is a `UserTaskMetric` later
  private[spark] override def toInfo(update: Option[Any], value: Option[Any]): AccumulableInfo = {
    // scalastyle:off println
      println("Info:: Name " + showMetricName + " update "
        + update.getOrElse("NULL") + " Value " + value.getOrElse("NULL"))
    // scalastyle:on

    new AccumulableInfo(
      id, name, update, value, true, true, Some(AccumulatorContext.SQL_ACCUM_IDENTIFIER))
  }



}


private[spark] object UserTaskMetrics {
  // val addr = System.identityHashCode(this)

  val userDefinedMetric: UserTaskMetric = {
    val sc = SparkContext.getOrCreate(SparkEnv.get.conf)
    val acc = new UserTaskMetric()
    acc.register(sc, name = Some("Unsafe User Defined Metric"),
      countFailedValues = false)
    acc
  }

  def registerWithTaskContext(acc: UserTaskMetric): UserTaskMetric = {
    if (acc.isAtDriverSide) {

      val taskContext = TaskContext.get()
      if (taskContext != null) {
        acc.metadata.name match {
          // scalastyle:off println
          case Some(x) => println("User Register Accumulator : " + x)
          // scalastyle:on
          case None => println("User Register Accumulator : None ")
        }
        taskContext.registerAccumulator(acc)
      }
    } else {
      acc.metadata.name match {
        // scalastyle:off println
        case Some(x) => println("User Driver Register Accumulator : " + x)
        // scalastyle:on
        case None => println("User Driver Register Accumulator : None ")
      }
    }
    acc
  }

  def createMetric(name: String): UserTaskMetric = {
    val sc = SparkContext.getOrCreate(SparkEnv.get.conf)



    val acc = new UserTaskMetric()
    acc.register(sc, name = Some(name), countFailedValues = false)
    println("EXEC ID : " + SparkEnv.get.executorId + " Name " +
    acc.metadata.name.getOrElse("NULL"))
    // registerWithTaskContext(acc)
    acc
  }

  def addMetrics(ctx: CodegenContext, ref: ArrayBuffer[Any]): Unit = {
    ref.zipWithIndex.foreach {
      case (obj, index) =>
        val userObj = obj.asInstanceOf[UserTaskMetric]
        ctx.addReferenceObj(
          userObj.name.getOrElse("usermetrics_" + index), obj)
    }
  }


  def metricTerm(ctx: CodegenContext, name: String, desc: String): String = {


    val acc = createMetric(desc)
    // scalastyle:off println
      println("Created New Object " + acc + "ID " + acc.metadata.id + " Addr : "
        + System.identityHashCode(acc))
    // scalastyle:on
    registerWithTaskContext(acc)
    ctx.addReferenceObj(name, acc )

  }

  def metricTermWithRegister(ctx: CodegenContext, name: String, desc: String): String = {


    val acc = if (SparkEnv.get.executorId == "driver") {
      UserTaskMetrics.registerWithTaskContext(createMetric(desc))
    } else {
      userDefinedMetric
    }

    // ctx.addReferenceObj(name, acc )
  }

  /**
    * A function that defines how we aggregate the final accumulator results among all tasks,
    * and represent it in string for a SQL physical operator.
    */
  def stringValue(valuesInput: Seq[Any]): String = {
    var valStr = ""
    val values = valuesInput.flatMap(valuesTmp => {
      // val vtmp = valuesInput.asInstanceOf[ArrayBuffer[String]].mkString("")
      valuesInput.asInstanceOf[ArrayBuffer[String]].map(vtmp => {
        // scalastyle:off println
        println("JOE " + vtmp)
        // scalastyle:on

        if (vtmp.contains(":")) {
          valStr = vtmp.split(":")(0)
          vtmp.split(":")(1).toLong
        } else {
          vtmp.toLong
        }
      })
    })

    val numberFormat = NumberFormat.getInstance()

    val validValues = values.filter(_ >= 0)
    val Seq(sum, min, med, max) = {
      val metric = if (validValues.isEmpty) {
        Seq.fill(4)(0L)
      } else {
        val sorted = validValues.sorted
        Seq(sorted.sum, sorted(0), sorted(validValues.length / 2), sorted(validValues.length - 1))
      }
      metric.map(numberFormat.format)
    }
    s"$valStr: $sum ($min, $med, $max)"
  }
}
