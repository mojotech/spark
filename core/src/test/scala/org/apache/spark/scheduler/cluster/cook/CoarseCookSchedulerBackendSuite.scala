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

package org.apache.spark.scheduler.cluster.cook

import java.util
import java.util.Collections
import java.util.UUID
import scala.collection.JavaConverters._

import org.apache.mesos.Protos.Value.Scalar
import org.apache.mesos.Protos._
import org.apache.mesos.{Protos, Scheduler, SchedulerDriver}
import org.mockito.Matchers._
import org.mockito.Mockito._
import org.mockito.Matchers
import org.scalatest.mock.MockitoSugar
import org.scalatest.BeforeAndAfter

import org.apache.spark.scheduler.TaskSchedulerImpl
import org.apache.spark.{LocalSparkContext, SparkConf, SparkContext, SecurityManager, SparkFunSuite}

import org.apache.spark.scheduler.cluster.cook

import com.twosigma.cook.jobclient.{ JobClient, JobClientException }

class CoarseCookSchedulerBackendSuite extends SparkFunSuite
    with LocalSparkContext
    with MockitoSugar
    with BeforeAndAfter {

  private def createSchedulerBackend(taskScheduler: TaskSchedulerImpl): CoarseCookSchedulerBackend = {
    val backend = new CoarseCookSchedulerBackend(
      taskScheduler, sc, "127.0.0.1", 12321, "vagrant", "ignorePassword"
    )
    backend.start()
    backend
  }

  var sparkConf: SparkConf = _

  before {
    sparkConf = (new SparkConf)
      .setMaster("local[*]")
      .setAppName("test-cook-dynamic-alloc")
      .setSparkHome("/path")

    sparkConf.set("spark.cores.max", "2")

    sc = new SparkContext(sparkConf)
  }

  test("isReady") {
    val taskScheduler = mock[TaskSchedulerImpl]
    when(taskScheduler.sc).thenReturn(sc)

    val backend = createSchedulerBackend(taskScheduler)

    assert(backend.isReady())
  }

  test("cook supports killing executors") {
    val taskScheduler = mock[TaskSchedulerImpl]
    when(taskScheduler.sc).thenReturn(sc)

    val backend = createSchedulerBackend(taskScheduler)
    val executorId = backend.executorsToJobIds.keySet.head
    var executorIds = Seq(executorId)

    assert(backend.doKillExecutors(executorIds))

    assert(!backend.executorsToJobIds.contains(executorId))
  }

  test("cook supports scaling executors down") {
    val taskScheduler = mock[TaskSchedulerImpl]
    when(taskScheduler.sc).thenReturn(sc)

    val backend = createSchedulerBackend(taskScheduler)
    val executorId = backend.executorsToJobIds.keySet.head
    var executorIds = Seq(executorId)

    backend.doKillExecutors(executorIds)
    assert(backend.executorsToJobIds.isEmpty)

    assert(backend.doRequestTotalExecutors(0))
    assert(backend.executorLimit == 0)

    backend.requestRemainingCores()

    assert(backend.executorsToJobIds.isEmpty)
  }

  test("cook doesn't update executor-job mapping when aborting a job fails") {
    val execId = "ex1"
    val jobId = UUID.randomUUID()

    val taskScheduler = mock[TaskSchedulerImpl]
    when(taskScheduler.sc).thenReturn(sc)

    val jobClientMock = mock[JobClient]
    when(jobClientMock.abort(List(jobId).asJavaCollection)).thenThrow(mock[JobClientException])

    val backend = new CoarseCookSchedulerBackend(
      taskScheduler, sc, "127.0.0.1", 12321, "vagrant", "ignorePassword"
    ) {
      override val jobClient = jobClientMock
    }

    backend.executorsToJobIds(execId) = jobId

    assert(backend.doKillExecutors(Seq(execId)))
    assert(backend.executorsToJobIds.contains(execId))
  }
}
