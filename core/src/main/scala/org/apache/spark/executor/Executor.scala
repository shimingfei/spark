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

package org.apache.spark.executor

import java.io.{File, NotSerializableException}
import java.lang.Thread.UncaughtExceptionHandler
import java.lang.management.ManagementFactory
import java.net.{URI, URL}
import java.nio.ByteBuffer
import java.util.{PriorityQueue => JPriorityQueue}
import java.util.Properties
import java.util.concurrent._
import javax.annotation.concurrent.GuardedBy

import scala.collection.JavaConverters._
import scala.collection.mutable.{ArrayBuffer, HashMap, Map}
import scala.util.control.NonFatal

import com.google.common.util.concurrent.ThreadFactoryBuilder

import org.apache.spark._
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.internal.Logging
import org.apache.spark.memory.TaskMemoryManager
import org.apache.spark.rpc.RpcTimeout
import org.apache.spark.scheduler._
import org.apache.spark.shuffle.FetchFailedException
import org.apache.spark.storage.{StorageLevel, TaskResultBlockId}
import org.apache.spark.util._
import org.apache.spark.util.io.ChunkedByteBuffer

/**
 * Spark executor, backed by a threadpool to run tasks.
 *
 * This can be used with Mesos, YARN, and the standalone scheduler.
 * An internal RPC interface is used for communication with the driver,
 * except in the case of Mesos fine-grained mode.
 */
private[spark] class Executor(
    executorId: String,
    executorHostname: String,
    env: SparkEnv,
    private val cores: Int,
    userClassPath: Seq[URL] = Nil,
    isLocal: Boolean = false,
    uncaughtExceptionHandler: UncaughtExceptionHandler = SparkUncaughtExceptionHandler)
  extends Logging {

  logInfo(s"Starting executor ID $executorId on host $executorHostname")

  // Application dependencies (added through SparkContext) that we've fetched so far on this node.
  // Each map holds the master's timestamp for the version of that file or JAR we got.
  private val currentFiles: HashMap[String, Long] = new HashMap[String, Long]()
  private val currentJars: HashMap[String, Long] = new HashMap[String, Long]()

  private val EMPTY_BYTE_BUFFER = ByteBuffer.wrap(new Array[Byte](0))

  private val conf = env.conf

  // No ip or host:port - just hostname
  Utils.checkHost(executorHostname)
  // must not have port specified.
  assert (0 == Utils.parseHostPort(executorHostname)._2)

  // Make sure the local hostname we report matches the cluster scheduler's name for this host
  Utils.setCustomHostname(executorHostname)

  if (!isLocal) {
    // Setup an uncaught exception handler for non-local mode.
    // Make any thread terminations due to uncaught exceptions kill the entire
    // executor process to avoid surprising stalls.
    Thread.setDefaultUncaughtExceptionHandler(uncaughtExceptionHandler)
  }

  // Start worker thread pool
  private val threadPool = {
    val threadFactory = new ThreadFactoryBuilder()
      .setDaemon(true)
      .setNameFormat("Executor task launch worker-%d")
      .setThreadFactory(new ThreadFactory {
        override def newThread(r: Runnable): Thread =
          // Use UninterruptibleThread to run tasks so that we can allow running codes without being
          // interrupted by `Thread.interrupt()`. Some issues, such as KAFKA-1894, HADOOP-10622,
          // will hang forever if some methods are interrupted.
          new UninterruptibleThread(r, "unused") // thread name will be set by ThreadFactoryBuilder
      })
      .build()
    Executors.newCachedThreadPool(threadFactory).asInstanceOf[ThreadPoolExecutor]
  }
  private val executorSource = new ExecutorSource(threadPool, executorId)
  // Pool used for threads that supervise task killing / cancellation
  private val taskReaperPool = ThreadUtils.newDaemonCachedThreadPool("Task reaper")
  // For tasks which are in the process of being killed, this map holds the most recently created
  // TaskReaper. All accesses to this map should be synchronized on the map itself (this isn't
  // a ConcurrentHashMap because we use the synchronization for purposes other than simply guarding
  // the integrity of the map's internal state). The purpose of this map is to prevent the creation
  // of a separate TaskReaper for every killTask() of a given task. Instead, this map allows us to
  // track whether an existing TaskReaper fulfills the role of a TaskReaper that we would otherwise
  // create. The map key is a task id.
  private val taskReaperForTask: HashMap[Long, TaskReaper] = HashMap[Long, TaskReaper]()

  if (!isLocal) {
    env.metricsSystem.registerSource(executorSource)
    env.blockManager.initialize(conf.getAppId)
  }

  // Whether to load classes in user jars before those in Spark jars
  private val userClassPathFirst = conf.getBoolean("spark.executor.userClassPathFirst", false)

  // Whether to monitor killed / interrupted tasks
  private val taskReaperEnabled = conf.getBoolean("spark.task.reaper.enabled", false)

  // Create our ClassLoader
  // do this after SparkEnv creation so can access the SecurityManager
  private val urlClassLoader = createClassLoader()
  private val replClassLoader = addReplClassLoaderIfNeeded(urlClassLoader)

  // Set the classloader for serializer
  env.serializer.setDefaultClassLoader(replClassLoader)

  // Max size of direct result. If task result is bigger than this, we use the block manager
  // to send the result back.
  private val maxDirectResultSize = Math.min(
    conf.getSizeAsBytes("spark.task.maxDirectResultSize", 1L << 20),
    RpcUtils.maxMessageSizeBytes(conf))

  // Limit of bytes for total size of results (default is 1GB)
  private val maxResultSize = Utils.getMaxResultSize(conf)

  // Maintains the list of running tasks.
  private val runningTasks = new ConcurrentHashMap[Long, TaskRunner]

  // Executor for the heartbeat task.
  private val heartbeater = ThreadUtils.newDaemonSingleThreadScheduledExecutor("driver-heartbeater")

  // must be initialized before running startDriverHeartbeat()
  private val heartbeatReceiverRef =
    RpcUtils.makeDriverRef(HeartbeatReceiver.ENDPOINT_NAME, conf, env.rpcEnv)

  // Case class that holds a task to be scheduled.
  // priority is a tuple representing (stageId, batchId, isReduceTask)
  private case class PendingTask(
    priority: (Int, Int, Boolean), tr: TaskRunner, scheduleTimeNanos: Long)

  private object PendingTaskOrdering extends Ordering[PendingTask] {
    def compare(a: PendingTask, b: PendingTask): Int = {
      if (a.priority._1 == b.priority._1) {
        // Prefer smaller batches first
        a.priority._2 compare b.priority._2
      } else {
        // When comparing tasks across stages, prefer reduce tasks from earlier stages
        if (a.priority._3 == b.priority._3) {
          a.priority._1 compare b.priority._1
        } else {
          -1 * (a.priority._3 compare b.priority._3)
        }

        // NOTE: Reduce stages get a higher stageId than the map stages they
        // depend on. But we should prioritize the reduce stage because we want
        // reduce stage of batch 0 to run before map stage of batch 1.
        // The reduce stage is only enqueued after all its dependencies are met !
        // -1 * (a.priority._1 compare b.priority._1)
      }
    }
  }

  private val taskSchedule = new JPriorityQueue[PendingTask](10240, PendingTaskOrdering)
  // Number of tasks currently being handled by the thread pool. The invariant is that this
  // is always <= cores provided in the constructor.
  private var currentActiveTasks = 0

  /**
   * When an executor is unable to send heartbeats to the driver more than `HEARTBEAT_MAX_FAILURES`
   * times, it should kill itself. The default value is 60. It means we will retry to send
   * heartbeats about 10 minutes because the heartbeat interval is 10s.
   */
  private val HEARTBEAT_MAX_FAILURES = conf.getInt("spark.executor.heartbeat.maxFailures", 60)

  /**
   * Count the failure times of heartbeat. It should only be accessed in the heartbeat thread. Each
   * successful heartbeat will reset it to 0.
   */
  private var heartbeatFailures = 0

  startDriverHeartbeater()

  private[executor] def numRunningTasks: Int = runningTasks.size()

  // Insert a task into the priority queue with the given priority
  private def scheduleTask(priority: (Int, Int, Boolean), tr: TaskRunner): Unit = {
    val entryTime = System.nanoTime
    taskSchedule.synchronized {
      taskSchedule.add(PendingTask(priority, tr, entryTime))
    }
    scheduleTasks()
  }

  // If there are slots free in the threadPool, this method picks the highest priority
  // task and executes it.
  private def scheduleTasks(): Unit = {
    taskSchedule.synchronized {
      while ((cores == 0 || currentActiveTasks < cores) && !taskSchedule.isEmpty()) {
        val t = taskSchedule.poll()
        currentActiveTasks += 1
        logInfo(
          s"Task ${t.tr.taskId} took ${(System.nanoTime - t.scheduleTimeNanos)/1e6} ms in queue")
        threadPool.execute(t.tr)
      }
    }
  }

  def notifyTaskEnd(taskId: Long): Unit = {
    taskSchedule.synchronized {
      currentActiveTasks -= 1
    }
    scheduleTasks()
  }

  def launchTask(context: ExecutorBackend, taskDescription: TaskDescription): Unit = {
    val tr = new TaskRunner(context, taskDescription)
    runningTasks.put(taskDescription.taskId, tr)
    // At this point we don't know the stageId or batchId, so schedule this task with the highest
    // priority.
    scheduleTask( (-1, -1, false), tr)
  }

  // Schedule a future task whose dependencies have been met ?
  def launchFutureTask(futureTaskRunner: TaskRunner, batchId: Int): Unit = {
    runningTasks.put(futureTaskRunner.taskId, futureTaskRunner)
    scheduleTask( (futureTaskRunner.task.stageId, batchId, true), futureTaskRunner)
  }

  def killTask(taskId: Long, interruptThread: Boolean, reason: String): Unit = {
    val taskRunner = runningTasks.get(taskId)
    if (taskRunner != null) {
      if (taskReaperEnabled) {
        val maybeNewTaskReaper: Option[TaskReaper] = taskReaperForTask.synchronized {
          val shouldCreateReaper = taskReaperForTask.get(taskId) match {
            case None => true
            case Some(existingReaper) => interruptThread && !existingReaper.interruptThread
          }
          if (shouldCreateReaper) {
            val taskReaper = new TaskReaper(
              taskRunner, interruptThread = interruptThread, reason = reason)
            taskReaperForTask(taskId) = taskReaper
            Some(taskReaper)
          } else {
            None
          }
        }
        // Execute the TaskReaper from outside of the synchronized block.
        maybeNewTaskReaper.foreach(taskReaperPool.execute)
      } else {
        taskRunner.kill(interruptThread = interruptThread, reason = reason)
      }
    }
  }

  /**
   * Function to kill the running tasks in an executor.
   * This can be called by executor back-ends to kill the
   * tasks instead of taking the JVM down.
   * @param interruptThread whether to interrupt the task thread
   */
  def killAllTasks(interruptThread: Boolean, reason: String) : Unit = {
    runningTasks.keys().asScala.foreach(t =>
      killTask(t, interruptThread = interruptThread, reason = reason))
  }

  def stop(): Unit = {
    env.metricsSystem.report()
    heartbeater.shutdown()
    heartbeater.awaitTermination(10, TimeUnit.SECONDS)
    threadPool.shutdown()
    if (!isLocal) {
      env.stop()
    }
  }

  /** Returns the total amount of time this JVM process has spent in garbage collection. */
  private def computeTotalGcTime(): Long = {
    ManagementFactory.getGarbageCollectorMXBeans.asScala.map(_.getCollectionTime).sum
  }

  /**
   * Class that facilitates running a task.
   * For batch tasks this class tracks which batch has been executed and which haven't.
   * For future tasks the run method is called twice: once to deserialize the task
   * and once to run it after dependencies have been met.
   */
  class TaskRunner(
      execBackend: ExecutorBackend,
      private val taskDescription: TaskDescription)
    extends Runnable {

    val taskId = taskDescription.taskId
    val threadName = s"Executor task launch worker for task $taskId"
    private val taskName = taskDescription.name

    /** If specified, this task has been killed and this option contains the reason. */
    @volatile private var reasonIfKilled: Option[String] = None

    @volatile private var threadId: Long = -1

    def getThreadId: Long = threadId

    /** Whether this task has been finished. */
    @GuardedBy("TaskRunner.this")
    private var finished = false

    def isFinished: Boolean = synchronized { finished }

    /** How much the JVM process has spent in GC when the task starts to run. */
    @volatile var startGCTime: Long = _

    /**
     * The task to run. This will be set in run() by deserializing the task binary coming
     * from the driver. Once it is set, it will never be changed.
     */
    @volatile var task: Task[Any] = _

    // For batch tasks this will hold the pointer to the BatchTask that was used.
    @volatile var batchTask: Task[Any] = _
    // For batch tasks this holds the unrolled Seq[Task]
    @volatile var tasks: Seq[Task[Any]] = _
    @volatile var lastExecutedBatch = -1
    @volatile var numBatches = 0
    @volatile var batchResults: Array[Any] = _

    val taskMemoryManager = new TaskMemoryManager(env.memoryManager, taskId)

    private var taskStart: Long = _

    private val ser = env.closureSerializer.newInstance()

    def kill(interruptThread: Boolean, reason: String): Unit = {
      logInfo(s"Executor is trying to kill $taskName (TID $taskId), reason: $reason")
      reasonIfKilled = Some(reason)
      if (task != null) {
        synchronized {
          if (!finished) {
            task.kill(interruptThread, reason)
          }
        }
      }
    }

    private def releaseLocksMemory(threwException: Boolean): Unit = {
      val releasedLocks = env.blockManager.releaseAllLocksForTask(taskId)
      val freedMemory = taskMemoryManager.cleanUpAllAllocatedMemory()

      if (freedMemory > 0 && !threwException) {
        val errMsg = s"Managed memory leak detected; size = $freedMemory bytes, TID = $taskId"
        if (conf.getBoolean("spark.unsafe.exceptionOnMemoryLeak", false)) {
          throw new SparkException(errMsg)
        } else {
          logWarning(errMsg)
        }
      }

      if (releasedLocks != null && releasedLocks.nonEmpty && !threwException) {
        val errMsg =
          s"${releasedLocks.size} block locks were not released by TID = $taskId:\n" +
            releasedLocks.mkString("[", ", ", "]")
        if (conf.getBoolean("spark.storage.exceptionOnPinLeak", false)) {
          throw new SparkException(errMsg)
        } else {
          logWarning(errMsg)
        }
      }
    }

    /**
     * Set the finished flag to true and clear the current thread's interrupt status
     * Also remove the task from runningTasks map. This is removed for successful tasks
     * by runDeserializedTask
     */
    private def setTaskFinishedAndClearInterruptStatus(): Unit = synchronized {
      this.finished = true
      // SPARK-14234 - Reset the interrupted status of the thread to avoid the
      // ClosedByInterruptException during execBackend.statusUpdate which causes
      // Executor to crash
      Thread.interrupted()
      releaseLocksMemory(true)
      runningTasks.remove(taskId)
      // Notify any waiting TaskReapers. Generally there will only be one reaper per task but there
      // is a rare corner-case where one task can have two reapers in case cancel(interrupt=False)
      // is followed by cancel(interrupt=True). Thus we use notifyAll() to avoid a lost wakeup:
      notifyAll()
    }

    protected def runAndHandleExceptions(function: () => Unit): Unit = {
      try {
        function()
      } catch {
        case t: Throwable if hasFetchFailure && !Utils.isFatalError(t) =>
          val reason = task.context.fetchFailed.get.toTaskFailedReason
          if (!t.isInstanceOf[FetchFailedException]) {
            // there was a fetch failure in the task, but some user code wrapped that exception
            // and threw something else.  Regardless, we treat it as a fetch failure.
            val fetchFailedCls = classOf[FetchFailedException].getName
            logWarning(s"TID ${taskId} encountered a ${fetchFailedCls} and " +
              s"failed, but the ${fetchFailedCls} was hidden by another " +
              s"exception.  Spark is handling this like a fetch failure and ignoring the " +
              s"other exception: $t")
          }
          setTaskFinishedAndClearInterruptStatus()
          execBackend.statusUpdate(taskId, TaskState.FAILED, ser.serialize(reason))

        case t: TaskKilledException =>
          logInfo(s"Executor killed $taskName (TID $taskId), reason: ${t.reason}")
          setTaskFinishedAndClearInterruptStatus()
          execBackend.statusUpdate(taskId, TaskState.KILLED, ser.serialize(TaskKilled(t.reason)))

        case _: InterruptedException | NonFatal(_) if
            task != null && task.reasonIfKilled.isDefined =>
          val killReason = task.reasonIfKilled.getOrElse("unknown reason")
          logInfo(s"Executor interrupted and killed $taskName (TID $taskId), reason: $killReason")
          setTaskFinishedAndClearInterruptStatus()
          execBackend.statusUpdate(
            taskId, TaskState.KILLED, ser.serialize(TaskKilled(killReason)))

        case cDE: CommitDeniedException =>
          val reason = cDE.toTaskFailedReason
          setTaskFinishedAndClearInterruptStatus()
          execBackend.statusUpdate(taskId, TaskState.FAILED, ser.serialize(reason))

        case t: Throwable =>
          // Attempt to exit cleanly by informing the driver of our failure.
          // If anything goes wrong (or this was a fatal exception), we will delegate to
          // the default uncaught exception handler, which will terminate the Executor.
          logError(s"Exception in $taskName (TID $taskId)", t)

          // Collect latest accumulator values to report back to the driver
          val accums: Seq[AccumulatorV2[_, _]] =
            if (task != null) {
              task.metrics.incExecutorRunTime(System.currentTimeMillis() - taskStart)
              task.metrics.incJvmGCTime(computeTotalGcTime() - startGCTime)
              task.collectAccumulatorUpdates(taskFailed = true)
            } else {
              Seq.empty
            }

          val accUpdates = accums.map(acc => acc.toInfo(Some(acc.value), None))

          val serializedTaskEndReason = {
            try {
              ser.serialize(new ExceptionFailure(t, accUpdates).withAccums(accums))
            } catch {
              case _: NotSerializableException =>
                // t is not serializable so just send the stacktrace
                ser.serialize(new ExceptionFailure(t, accUpdates, false).withAccums(accums))
            }
          }
          setTaskFinishedAndClearInterruptStatus()
          execBackend.statusUpdate(taskId, TaskState.FAILED, serializedTaskEndReason)

          // Don't forcibly exit unless the exception was inherently fatal, to avoid
          // stopping other tasks unnecessarily.
          if (Utils.isFatalError(t)) {
            uncaughtExceptionHandler.uncaughtException(Thread.currentThread(), t)
          }
      } finally {
        notifyTaskEnd(taskId)
      }
    }

    // Deserializes the task and stores it in the `task` variable.
    private def deserializeTask(): Unit = {
      val deserializeStartTime = System.currentTimeMillis()

      // Must be set before updateDependencies() is called, in case fetching dependencies
      // requires access to properties contained within (e.g. for access control).
      Executor.taskDeserializationProps.set(taskDescription.properties)

      updateDependencies(taskDescription.addedFiles, taskDescription.addedJars)
      task = ser.deserialize[Task[Any]](taskDescription.serializedTask,
        Thread.currentThread.getContextClassLoader)
      task.localProperties = taskDescription.properties

      // If this task has been killed before we deserialized it, let's quit now. Otherwise,
      // continue executing the task.
      if (reasonIfKilled.isDefined) {
        // Throw an exception rather than returning, because returning within a try{} block
        // causes a NonLocalReturnControl exception to be thrown. The NonLocalReturnControl
        // exception will be caught by the catch block, leading to an incorrect ExceptionFailure
        // for the task.
        throw new TaskKilledException(reasonIfKilled.get)
      }

      task.prepTaskWithContext(taskId, taskDescription.attemptNumber, env.metricsSystem)
      val deserializeStopTime = System.currentTimeMillis()
      logInfo(s"Deserializing Task ${taskId} took " + (deserializeStopTime - deserializeStartTime))
      task.metrics.setExecutorDeserializeTime(deserializeStopTime - deserializeStartTime)
    }

    private def runDeserializedTask(): Unit = {
      if (numBatches == 0 || lastExecutedBatch == 0) {
        execBackend.statusUpdate(taskId, TaskState.RUNNING, EMPTY_BYTE_BUFFER)
      }
      task.setTaskMemoryManager(taskMemoryManager)

      // TODO(shivaram): Is this the right thing to do for Drizzle ??
      logDebug("Task " + taskId + "'s epoch is " + task.epoch)
      env.mapOutputTracker.updateEpoch(task.epoch)

      val startGCTime = computeTotalGcTime()

      // Run the actual task and measure its runtime.
      taskStart = System.currentTimeMillis()
      var threwException = true
      val value = task.run(
        taskAttemptId = taskId,
        attemptNumber = taskDescription.attemptNumber,
        metricsSystem = env.metricsSystem,
        batchId = lastExecutedBatch)
      threwException = false
      val taskFinish = System.currentTimeMillis()
      logInfo(s"Finished running $taskName (TID $taskId) took ${taskFinish-taskStart}")

      // If the task has been killed, let's fail it.
      task.context.killTaskIfInterrupted()

      if (numBatches > 0) {
        batchResults(lastExecutedBatch) = value
        batchTask.metrics.incExecutorRunTime((taskFinish - taskStart))
        batchTask.metrics.incJvmGCTime(computeTotalGcTime() - startGCTime)
        val y = task.metrics.shuffleReadMetrics
        logInfo(s"TID $taskId, st ${task.stageId}, b $lastExecutedBatch, fw ${y.fetchWaitTime}")
      } else {
        task.metrics.incExecutorRunTime((taskFinish - taskStart))
        task.metrics.incJvmGCTime(computeTotalGcTime() - startGCTime)
      }

      if (numBatches == 0 || (lastExecutedBatch == numBatches - 1)) {
        val resultSer = env.serializer.newInstance()
        val beforeSerialization = System.currentTimeMillis()
        val valueBytes = if (numBatches == 0) {
          resultSer.serialize(value)
        } else {
          resultSer.serialize(batchResults)
        }
        val afterSerialization = System.currentTimeMillis()

        // Note: accumulator updates must be collected after TaskMetrics is updated
        val accumUpdates = if (numBatches == 0) {
          task.metrics.setResultSerializationTime(afterSerialization - beforeSerialization)
          task.collectAccumulatorUpdates()
        } else {
          batchTask.metrics.setResultSerializationTime(afterSerialization - beforeSerialization)
          batchTask.collectAccumulatorUpdates()
        }

        // TODO: do not serialize value twice
        val directResult = new DirectTaskResult(valueBytes, accumUpdates)
        val serializedDirectResult = ser.serialize(directResult)
        val resultSize = serializedDirectResult.limit

        // directSend = sending directly back to the driver
        val serializedResult: ByteBuffer = {
          if (maxResultSize > 0 && resultSize > maxResultSize) {
            logWarning(s"Finished $taskName (TID $taskId). Result is larger than maxResultSize " +
              s"(${Utils.bytesToString(resultSize)} > ${Utils.bytesToString(maxResultSize)}), " +
              s"dropping it.")
            ser.serialize(new IndirectTaskResult[Any](TaskResultBlockId(taskId), resultSize))
          } else if (resultSize > maxDirectResultSize) {
            val blockId = TaskResultBlockId(taskId)
            env.blockManager.putBytes(
              blockId,
              new ChunkedByteBuffer(serializedDirectResult.duplicate()),
              StorageLevel.MEMORY_AND_DISK_SER)
            logInfo(
              s"Finished $taskName (TID $taskId). $resultSize bytes result sent via BlockManager)")
            ser.serialize(new IndirectTaskResult[Any](blockId, resultSize))
          } else {
            logInfo(s"Finished $taskName (TID $taskId). $resultSize bytes result sent to driver")
            serializedDirectResult
          }
        }

        releaseLocksMemory(false)
        execBackend.statusUpdate(taskId, TaskState.FINISHED, serializedResult)
        runningTasks.remove(taskId)

      } else {
        // If the next task in the batch is a future task queue it or schedule it
        if (tasks(lastExecutedBatch + 1).isFutureTask) {
          queueFutureTask(tasks(lastExecutedBatch + 1), lastExecutedBatch + 1)
        } else {
          scheduleTask( (task.stageId, lastExecutedBatch + 1, false), this)
        }
      }
    }

    private def queueFutureTask(ft: Task[Any], batchId: Int): Unit = {
      // TODO: We should handle multiple shuffle deps by just submitting all of them
      // to the future task waiter.
      // Assume all future tasks are batch tasks ??

      val taskToUse = if (batchTask != null) {
        batchTask
      } else {
        ft
      }
      // assert(batchTask != null)
      val depShuffles = taskToUse.depShuffleIds.map { x =>
        x.map(_(batchId))
      }.getOrElse(Seq.empty)

      val depNumMaps = taskToUse.depShuffleNumMaps.getOrElse(Seq.empty)
      assert(depNumMaps.size == depShuffles.size)

      if (depShuffles.isEmpty) {
        // If we get a future task with no dependency just put it in the task running queue
        launchFutureTask(this, batchId)
      } else {
        if (depShuffles.size != 1) {
          logWarning("For future task " + ft + " batch " + batchId + ", got depShuffles " +
            depShuffles.mkString(",") + " using only the first")
        }

        val shuffleDepId = depShuffles.head
        val shuffleNumMaps = depNumMaps.head
        env.futureTaskWaiter.submitFutureTask(FutureTaskInfo(
          shuffleDepId,
          shuffleNumMaps,
          ft.partitionId,
          taskId,
          None,
          () => launchFutureTask(this, batchId)))
        // TODO(shivaram): Should we send some status update here ?
      }
    }

    override def run(): Unit = {
      Thread.currentThread.setContextClassLoader(replClassLoader)

      runAndHandleExceptions(() => {
        if (task == null) {
          logInfo(s"Deserializing $taskName (TID $taskId)")
          deserializeTask()

          if (task.isInstanceOf[BatchTask]) {
            tasks = task.asInstanceOf[BatchTask].getTasks()
            batchTask = task // save a reference to original task
            numBatches = tasks.length
            batchResults = new Array[Any](numBatches)
            // Set the first task as the one to be run now
            task = tasks(0)
          }

          // Run or queue the task
          if (task.isFutureTask) {
            queueFutureTask(task, 0)
          } else {
            lastExecutedBatch = lastExecutedBatch + 1
            runDeserializedTask()
          }
        } else {
          // The task was already deserialized, meaning that this TaskRunner is for a task
          // that can now be run.
          if (numBatches != 0) {
            lastExecutedBatch = lastExecutedBatch + 1
            logInfo(s"For TID $taskId, start executing $lastExecutedBatch")
            task = tasks(lastExecutedBatch)
          }
          logInfo(s"Running $taskName (TID $taskId)")

          if (reasonIfKilled.isDefined) {
            // Throw an exception rather than returning, because returning within a try{} block
            // causes a NonLocalReturnControl exception to be thrown. The NonLocalReturnControl
            // exception will be caught by the catch block, leading to an incorrect ExceptionFailure
            // for the task.
            throw new TaskKilledException(reasonIfKilled.get)
          }

          runDeserializedTask()
        }
      })
    }

    private def hasFetchFailure: Boolean = {
      task != null && task.context != null && task.context.fetchFailed.isDefined
    }
  }

  /**
   * Supervises the killing / cancellation of a task by sending the interrupted flag, optionally
   * sending a Thread.interrupt(), and monitoring the task until it finishes.
   *
   * Spark's current task cancellation / task killing mechanism is "best effort" because some tasks
   * may not be interruptable or may not respond to their "killed" flags being set. If a significant
   * fraction of a cluster's task slots are occupied by tasks that have been marked as killed but
   * remain running then this can lead to a situation where new jobs and tasks are starved of
   * resources that are being used by these zombie tasks.
   *
   * The TaskReaper was introduced in SPARK-18761 as a mechanism to monitor and clean up zombie
   * tasks. For backwards-compatibility / backportability this component is disabled by default
   * and must be explicitly enabled by setting `spark.task.reaper.enabled=true`.
   *
   * A TaskReaper is created for a particular task when that task is killed / cancelled. Typically
   * a task will have only one TaskReaper, but it's possible for a task to have up to two reapers
   * in case kill is called twice with different values for the `interrupt` parameter.
   *
   * Once created, a TaskReaper will run until its supervised task has finished running. If the
   * TaskReaper has not been configured to kill the JVM after a timeout (i.e. if
   * `spark.task.reaper.killTimeout < 0`) then this implies that the TaskReaper may run indefinitely
   * if the supervised task never exits.
   */
  private class TaskReaper(
      taskRunner: TaskRunner,
      val interruptThread: Boolean,
      val reason: String)
    extends Runnable {

    private[this] val taskId: Long = taskRunner.taskId

    private[this] val killPollingIntervalMs: Long =
      conf.getTimeAsMs("spark.task.reaper.pollingInterval", "10s")

    private[this] val killTimeoutMs: Long = conf.getTimeAsMs("spark.task.reaper.killTimeout", "-1")

    private[this] val takeThreadDump: Boolean =
      conf.getBoolean("spark.task.reaper.threadDump", true)

    override def run(): Unit = {
      val startTimeMs = System.currentTimeMillis()
      def elapsedTimeMs = System.currentTimeMillis() - startTimeMs
      def timeoutExceeded(): Boolean = killTimeoutMs > 0 && elapsedTimeMs > killTimeoutMs
      try {
        // Only attempt to kill the task once. If interruptThread = false then a second kill
        // attempt would be a no-op and if interruptThread = true then it may not be safe or
        // effective to interrupt multiple times:
        taskRunner.kill(interruptThread = interruptThread, reason = reason)
        // Monitor the killed task until it exits. The synchronization logic here is complicated
        // because we don't want to synchronize on the taskRunner while possibly taking a thread
        // dump, but we also need to be careful to avoid races between checking whether the task
        // has finished and wait()ing for it to finish.
        var finished: Boolean = false
        while (!finished && !timeoutExceeded()) {
          taskRunner.synchronized {
            // We need to synchronize on the TaskRunner while checking whether the task has
            // finished in order to avoid a race where the task is marked as finished right after
            // we check and before we call wait().
            if (taskRunner.isFinished) {
              finished = true
            } else {
              taskRunner.wait(killPollingIntervalMs)
            }
          }
          if (taskRunner.isFinished) {
            finished = true
          } else {
            logWarning(s"Killed task $taskId is still running after $elapsedTimeMs ms")
            if (takeThreadDump) {
              try {
                Utils.getThreadDumpForThread(taskRunner.getThreadId).foreach { thread =>
                  if (thread.threadName == taskRunner.threadName) {
                    logWarning(s"Thread dump from task $taskId:\n${thread.stackTrace}")
                  }
                }
              } catch {
                case NonFatal(e) =>
                  logWarning("Exception thrown while obtaining thread dump: ", e)
              }
            }
          }
        }

        if (!taskRunner.isFinished && timeoutExceeded()) {
          if (isLocal) {
            logError(s"Killed task $taskId could not be stopped within $killTimeoutMs ms; " +
              "not killing JVM because we are running in local mode.")
          } else {
            // In non-local-mode, the exception thrown here will bubble up to the uncaught exception
            // handler and cause the executor JVM to exit.
            throw new SparkException(
              s"Killing executor JVM because killed task $taskId could not be stopped within " +
                s"$killTimeoutMs ms.")
          }
        }
      } finally {
        // Clean up entries in the taskReaperForTask map.
        taskReaperForTask.synchronized {
          taskReaperForTask.get(taskId).foreach { taskReaperInMap =>
            if (taskReaperInMap eq this) {
              taskReaperForTask.remove(taskId)
            } else {
              // This must have been a TaskReaper where interruptThread == false where a subsequent
              // killTask() call for the same task had interruptThread == true and overwrote the
              // map entry.
            }
          }
        }
      }
    }
  }

  /**
   * Create a ClassLoader for use in tasks, adding any JARs specified by the user or any classes
   * created by the interpreter to the search path
   */
  private def createClassLoader(): MutableURLClassLoader = {
    // Bootstrap the list of jars with the user class path.
    val now = System.currentTimeMillis()
    userClassPath.foreach { url =>
      currentJars(url.getPath().split("/").last) = now
    }

    val currentLoader = Utils.getContextOrSparkClassLoader

    // For each of the jars in the jarSet, add them to the class loader.
    // We assume each of the files has already been fetched.
    val urls = userClassPath.toArray ++ currentJars.keySet.map { uri =>
      new File(uri.split("/").last).toURI.toURL
    }
    if (userClassPathFirst) {
      new ChildFirstURLClassLoader(urls, currentLoader)
    } else {
      new MutableURLClassLoader(urls, currentLoader)
    }
  }

  /**
   * If the REPL is in use, add another ClassLoader that will read
   * new classes defined by the REPL as the user types code
   */
  private def addReplClassLoaderIfNeeded(parent: ClassLoader): ClassLoader = {
    val classUri = conf.get("spark.repl.class.uri", null)
    if (classUri != null) {
      logInfo("Using REPL class URI: " + classUri)
      try {
        val _userClassPathFirst: java.lang.Boolean = userClassPathFirst
        val klass = Utils.classForName("org.apache.spark.repl.ExecutorClassLoader")
          .asInstanceOf[Class[_ <: ClassLoader]]
        val constructor = klass.getConstructor(classOf[SparkConf], classOf[SparkEnv],
          classOf[String], classOf[ClassLoader], classOf[Boolean])
        constructor.newInstance(conf, env, classUri, parent, _userClassPathFirst)
      } catch {
        case _: ClassNotFoundException =>
          logError("Could not find org.apache.spark.repl.ExecutorClassLoader on classpath!")
          System.exit(1)
          null
      }
    } else {
      parent
    }
  }

  /**
   * Download any missing dependencies if we receive a new set of files and JARs from the
   * SparkContext. Also adds any new JARs we fetched to the class loader.
   */
  private def updateDependencies(newFiles: Map[String, Long], newJars: Map[String, Long]) {
    lazy val hadoopConf = SparkHadoopUtil.get.newConfiguration(conf)
    synchronized {
      // Fetch missing dependencies
      for ((name, timestamp) <- newFiles if currentFiles.getOrElse(name, -1L) < timestamp) {
        logInfo("Fetching " + name + " with timestamp " + timestamp)
        // Fetch file with useCache mode, close cache for local mode.
        Utils.fetchFile(name, new File(SparkFiles.getRootDirectory()), conf,
          env.securityManager, hadoopConf, timestamp, useCache = !isLocal)
        currentFiles(name) = timestamp
      }
      for ((name, timestamp) <- newJars) {
        val localName = new URI(name).getPath.split("/").last
        val currentTimeStamp = currentJars.get(name)
          .orElse(currentJars.get(localName))
          .getOrElse(-1L)
        if (currentTimeStamp < timestamp) {
          logInfo("Fetching " + name + " with timestamp " + timestamp)
          // Fetch file with useCache mode, close cache for local mode.
          Utils.fetchFile(name, new File(SparkFiles.getRootDirectory()), conf,
            env.securityManager, hadoopConf, timestamp, useCache = !isLocal)
          currentJars(name) = timestamp
          // Add it to our class loader
          val url = new File(SparkFiles.getRootDirectory(), localName).toURI.toURL
          if (!urlClassLoader.getURLs().contains(url)) {
            logInfo("Adding " + url + " to class loader")
            urlClassLoader.addURL(url)
          }
        }
      }
    }
  }

  /** Reports heartbeat and metrics for active tasks to the driver. */
  private def reportHeartBeat(): Unit = {
    // list of (task id, accumUpdates) to send back to the driver
    val accumUpdates = new ArrayBuffer[(Long, Seq[AccumulatorV2[_, _]])]()
    val curGCTime = computeTotalGcTime()

    for (taskRunner <- runningTasks.values().asScala) {
      if (taskRunner.task != null) {
        taskRunner.task.metrics.mergeShuffleReadMetrics()
        taskRunner.task.metrics.setJvmGCTime(curGCTime - taskRunner.startGCTime)
        accumUpdates += ((taskRunner.taskId, taskRunner.task.metrics.accumulators()))
      }
    }

    val message = Heartbeat(executorId, accumUpdates.toArray, env.blockManager.blockManagerId)
    try {
      val response = heartbeatReceiverRef.askSync[HeartbeatResponse](
          message, RpcTimeout(conf, "spark.executor.heartbeatInterval", "10s"))
      if (response.reregisterBlockManager) {
        logInfo("Told to re-register on heartbeat")
        env.blockManager.reregister()
      }
      heartbeatFailures = 0
    } catch {
      case NonFatal(e) =>
        logWarning("Issue communicating with driver in heartbeater", e)
        heartbeatFailures += 1
        if (heartbeatFailures >= HEARTBEAT_MAX_FAILURES) {
          logError(s"Exit as unable to send heartbeats to driver " +
            s"more than $HEARTBEAT_MAX_FAILURES times")
          System.exit(ExecutorExitCode.HEARTBEAT_FAILURE)
        }
    }
  }

  /**
   * Schedules a task to report heartbeat and partial metrics for active tasks to driver.
   */
  private def startDriverHeartbeater(): Unit = {
    val intervalMs = conf.getTimeAsMs("spark.executor.heartbeatInterval", "10s")

    // Wait a random interval so the heartbeats don't end up in sync
    val initialDelay = intervalMs + (math.random * intervalMs).asInstanceOf[Int]

    val heartbeatTask = new Runnable() {
      override def run(): Unit = Utils.logUncaughtExceptions(reportHeartBeat())
    }
    heartbeater.scheduleAtFixedRate(heartbeatTask, initialDelay, intervalMs, TimeUnit.MILLISECONDS)
  }
}

private[spark] object Executor {
  // This is reserved for internal use by components that need to read task properties before a
  // task is fully deserialized. When possible, the TaskContext.getLocalProperty call should be
  // used instead.
  val taskDeserializationProps: ThreadLocal[Properties] = new ThreadLocal[Properties]
}
