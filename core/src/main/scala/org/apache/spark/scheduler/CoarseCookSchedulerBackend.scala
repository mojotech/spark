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

package org.apache.spark.scheduler

import java.io.{ BufferedWriter, FileWriter }
import java.net.URI
import java.nio.file.{ Files, Paths }
import java.util.UUID

import scala.collection.JavaConverters._

import org.apache.mesos._
import org.apache.mesos.Protos._

import org.apache.spark.{Logging, SparkContext}
import org.apache.spark.scheduler.cluster.CoarseGrainedSchedulerBackend
import org.apache.spark.scheduler.cluster.mesos.CoarseMesosSchedulerBackend

import com.twosigma.cook.jobclient.{Job, FetchableURI, JobClient, JobListener => CJobListener}

object CoarseCookSchedulerBackend {
  def fetchUri(uri: String): String =
    Option(URI.create(uri).getScheme).map(_.toLowerCase) match {
      case Some("http") => s"curl $uri"
      case None | Some("file") => s"cp $uri ."
      case Some(x) => sys.error(s"$x not supported yet")
    }
}

/**
 * A SchedulerBackend that runs tasks using Cook, using "coarse-grained" tasks, where it holds
 * onto Cook instances for the duration of the Spark job instead of relinquishing cores whenever
 * a task is done. It launches Spark tasks within the coarse-grained Cook instances using the
 * CoarseGrainedSchedulerBackend mechanism. This class is useful for lower and more predictable
 * latency.
 */
private[spark] class CoarseCookSchedulerBackend(
  scheduler: TaskSchedulerImpl,
  sc: SparkContext,
  cookHost: String,
  cookPort: Int,
  cookUser: String,
  cookPassword: String
) extends CoarseGrainedSchedulerBackend(scheduler, sc.env.rpcEnv) with Logging {
  // Book-keeping for cores we have requested from cook
  var totalCoresRequested = 0
  var totalFailures = 0
  val maxFailures = conf.getInt("spark.executor.failures", 5)

  val maxCores = conf.getInt("spark.cores.max", 0)
  val maxCoresPerJob = conf.getInt("spark.cook.cores.per.job.max", 5)

  val priority = conf.getInt("spark.cook.priority", 75)

  val executorUuidWriter: UUID => Unit =
    conf.getOption("spark.cook.executoruuid.log").fold { _: UUID => () } { _file =>
      def file(ct: Int) = s"${_file}.$ct"
      def path(ct: Int) = Paths.get(file(ct))

      // Here we roll existing logs
      @annotation.tailrec
      def findFirstFree(ct: Int = 0): Int =
        if (Files.exists(path(ct))) findFirstFree(ct + 1)
        else ct

      @annotation.tailrec
      def rollin(ct: Int) {
        if (ct > 0) {
          Files.move(path(ct - 1), path(ct))
          rollin(ct - 1)
        }
      }

      rollin(findFirstFree())

      { uuid: UUID =>
        val bw = new BufferedWriter(new FileWriter(file(0), true))
        bw.write(uuid.toString)
        bw.newLine()
        bw.close()
      }
    }

  // TODO do we need to do something smarter about the security manager?
  val sparkMesosScheduler =
    new CoarseMesosSchedulerBackend(scheduler, sc, "", sc.env.securityManager)

  val jobClient = {
      val builder = new JobClient.Builder()
          .setHost(cookHost)
          .setPort(cookPort)
          .setEndpoint("rawscheduler")
          .setStatusUpdateInterval(1)
          .setBatchRequestSize(10)
      Option(cookUser) match {
          case Some(user) => builder.setUsernameAuth(user, cookPassword)
          case None => builder.setKerberosAuth()
      }
      builder.build()
  }

  var runningJobUUIDs = Set[UUID]()

  val jobListener = new CJobListener {
    // These are called serially so don't need to worry about race conditions
    def onStatusUpdate(job : Job) {
      if (job.getStatus == Job.Status.COMPLETED && !job.isSuccess) {
        totalCoresRequested -= job.getCpus().toInt
        totalFailures += 1
        logWarning(s"Job ${job.getUUID} has died. Failure ($totalFailures/$maxFailures)")
        runningJobUUIDs = runningJobUUIDs - job.getUUID
        if (totalFailures >= maxFailures) {
          // TODO should we abort the outstanding tasks now?
          logError(s"We have exceeded our maximum failures ($maxFailures)" +
                    "and will not relaunch any more tasks")
        } else requestRemainingCores()
      }
    }
  }

  def createJob(numCores: Int): Job = { // should return Job
    import CoarseCookSchedulerBackend.fetchUri

    val jobId = UUID.randomUUID()
    executorUuidWriter(jobId)
    logInfo(s"Creating job with id: $jobId")
    val fakeOffer = Offer.newBuilder()
      .setId(OfferID.newBuilder().setValue("Cook-id"))
      .setFrameworkId(FrameworkID.newBuilder().setValue("Cook"))
      .setHostname("$(hostname)")
      .setSlaveId(SlaveID.newBuilder().setValue(jobId.toString))
      .build()
    val taskId = sparkMesosScheduler.newMesosTaskId()
    val commandInfo = sparkMesosScheduler.createCommand(fakeOffer, numCores, taskId)
    val commandString = commandInfo.getValue
    val environmentInfo = commandInfo.getEnvironment
    // Note that it is critical to export these variables otherwise when
    // we invoke the spark scripts, our values will not be picked up
    val environment =
      environmentInfo.getVariablesList.asScala
      .map{ v => (v.getName, v.getValue) }.toMap +
      ("SPARK_LOCAL_DIRS" -> "spark-temp")

    val uris = commandInfo.getUrisList.asScala
      .map{ uri => new FetchableURI.Builder()
                       .setValue(uri.getValue)
                       .setExtract(uri.getExtract)
                       .setExecutable(uri.getExecutable)
                       .build()
       }
    logDebug(s"command: $commandString")
    val remoteHdfsConf = conf.get("spark.executor.cook.hdfs.conf.remote", "")
    val remoteConfFetch = if (remoteHdfsConf.nonEmpty) {
      val name = Paths.get(remoteHdfsConf).getFileName
      Seq(
        fetchUri(remoteHdfsConf),
        "mkdir HADOOP_CONF_DIR",
        s"tar --strip-components=1 -xvzf $name -C HADOOP_CONF_DIR",
        // This must be absolute because we cd into the spark directory
        s"export HADOOP_CONF_DIR=`pwd`/HADOOP_CONF_DIR",
        "export HADOOP_CLASSPATH=$HADOOP_CONF_DIR"
      )

    } else Seq()

    val cleanup = "if [ -z $KEEP_SPARK_LOCAL_DIRS ]; then rm -rf $SPARK_LOCAL_DIRS; echo deleted $SPARK_LOCAL_DIRS; fi"

    val cmds = remoteConfFetch ++ environment ++ Seq(commandString, cleanup)

    new Job.Builder()
      .setUUID(jobId)
      .setEnv(environment.asJava)
      .setUris(uris.asJava)
      .setCommand(cmds.mkString("; "))
      .setMemory(sparkMesosScheduler.calculateTotalMemory(sc))
      .setCpus(numCores)
      .setPriority(priority)
      .build()
  }

  def createRemainingJobs(): List[Job] = {
    def loop(coresRemaining: Int, jobs: List[Job]): List[Job] =
      if (coresRemaining <= 0) jobs
      else if (coresRemaining <= maxCoresPerJob) createJob(coresRemaining) :: jobs
      else loop(coresRemaining - maxCoresPerJob, createJob(maxCoresPerJob) :: jobs)
    loop(maxCores - totalCoresRequested, Nil).reverse
  }

  def requestRemainingCores() : Unit = {
    val jobs = createRemainingJobs()
    totalCoresRequested += jobs.map(_.getCpus.toInt).sum
    runningJobUUIDs = runningJobUUIDs ++ jobs.map(_.getUUID)
    jobClient.submit(jobs.asJava, jobListener)
  }

  // TODO we should consider making this async, because if there are issues with cook it'll
  // just block the spark shell completely. We can block if they submit something (we don't
  // want to get into thread management issues)
  override def start() {
    super.start()
    logInfo("Starting Cook Spark Scheduler")
    requestRemainingCores()
  }

  override def stop() {
    super.stop()
    jobClient.abort(runningJobUUIDs.asJava)
  }

  private[this] val minExecutorsNecessary =
    math.ceil(maxCores.toDouble / maxCoresPerJob) * minRegisteredRatio

  override def sufficientResourcesRegistered(): Boolean =
    totalRegisteredExecutors.get >= minExecutorsNecessary

  private[this] var lastIsReadyLog = 0L

  override def isReady(): Boolean = {
    val ret = super.isReady()
    val cur = System.currentTimeMillis
    if (!ret && cur - lastIsReadyLog > 5000) {
      logInfo("Backend is not yet ready. Registered executors " +
        s"[${totalRegisteredExecutors.get}] vs minimum necessary " +
        s"to start [$minExecutorsNecessary]")
      lastIsReadyLog = cur
    }
    ret
  }
}
