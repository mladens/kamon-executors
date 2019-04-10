/* =========================================================================================
 * Copyright © 2013-2017 the kamon project <http://kamon.io/>
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 * =========================================================================================
 */

package kamon
package executors

import java.util.concurrent.{Callable, ExecutorService, ThreadPoolExecutor, TimeUnit, ForkJoinPool => JavaForkJoinPool}

import kamon.jsr166.LongAdder

import scala.concurrent.forkjoin.{ForkJoinPool => ScalaForkJoinPool}
import kamon.metric.Counter
import kamon.tag.TagSet
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._
import scala.util.Try
import java.io.Closeable

object Executors {
  private val logger = LoggerFactory.getLogger("kamon.executors.Executors")

  private val DelegatedExecutor = Class.forName("java.util.concurrent.Executors$DelegatedExecutorService")
  private val FinalizableDelegated = Class.forName("java.util.concurrent.Executors$FinalizableDelegatedExecutorService")
  private val DelegateScheduled = Class.forName("java.util.concurrent.Executors$DelegatedScheduledExecutorService")
  private val JavaFJP = classOf[JavaForkJoinPool]
  private val ScalaFJP = classOf[ScalaForkJoinPool]
  private val InstrumentedExecutor = classOf[InstrumentedExecutorService[_]]


  /**
    *   Extension trait for externally instrumented ExecutorService instances. Meant to be used when a ExecutorService
    *   is not assignable to either ThreadPoolExecutor or Scala/Java ForkJoinPool.
    */
  trait ExecutorSampler {
    /**
      *   Collects all metrics from the ExecutorService and records them in Kamon's instruments.
      */
    def sample(): Unit

    /**
      *   Cleanup any state that is no longer required after the ExecutorService is no longer being monitored.
      */
    def cleanup(): Unit
  }

  def register(name: String, executor: ExecutorService): Closeable =
    register(name, TagSet.Empty, executor)

  def register(name: String, tags: TagSet, executor: ExecutorService): Closeable = executor match {
    case executor: ExecutorService if isAssignableTo(executor, DelegatedExecutor)     => register(name, tags, unwrap(executor))
    case executor: ExecutorService if isAssignableTo(executor, FinalizableDelegated)  => register(name, tags, unwrap(executor))
    case executor: ExecutorService if isAssignableTo(executor, DelegateScheduled)     => register(name, tags, unwrap(executor))
    case threadPool: ThreadPoolExecutor                                               => register(name, tags, threadPoolSampler(name, tags, threadPool))
    case executor: ExecutorService if isAssignableTo(executor, classOf[InstrumentedExecutorService[_]])  =>
      register(name, tags, forkJoinPoolSampler(name, tags, executor.asInstanceOf[InstrumentedExecutorService[_]]))

    case anyOther                       =>
      logger.error("Cannot register unsupported executor service [{}]", anyOther)
      fakeRegistration
  }

  def register(name: String, tags: TagSet, sampler: ExecutorSampler): Closeable = {
    val samplingInterval = Kamon.config().getDuration("kamon.executors.sample-interval")
    val scheduledFuture = Kamon.scheduler().scheduleAtFixedRate(sampleTask(sampler), samplingInterval.toMillis, samplingInterval.toMillis, TimeUnit.MILLISECONDS)

    new Closeable {
      override def close(): Unit = {
        Try {
          scheduledFuture.cancel(false)
          sampler.cleanup()
        }.failed.map { ex =>
          logger.error(s"Failed to cancel registration for executor [name=${name}, tags=${tags.toString()}]", ex)
        }
        ()
      }
    }
  }

  private val fakeRegistration = new Closeable {
    override def close(): Unit = ()
  }

  private def isAssignableTo(executor: ExecutorService, expectedClass: Class[_]): Boolean =
    expectedClass.isAssignableFrom(executor.getClass)

  private def threadPoolSampler(name: String, tags: TagSet, pool: ThreadPoolExecutor): ExecutorSampler = new ExecutorSampler {
    val poolInstruments = Instruments.threadPool(name, tags)
    val taskCountSource = Counter.delta(() => pool.getTaskCount)
    val completedTaskCountSource = Counter.delta(() => pool.getCompletedTaskCount)

    def sample(): Unit = {
      poolInstruments.poolMin.update(pool.getCorePoolSize)
      poolInstruments.poolMax.update(pool.getMaximumPoolSize)
      poolInstruments.poolSize.record(pool.getPoolSize)
      poolInstruments.activeThreads.record(pool.getActiveCount)
      taskCountSource.accept(poolInstruments.submittedTasks)
      completedTaskCountSource.accept(poolInstruments.processedTasks)
      poolInstruments.queuedTasks.record(pool.getQueue.size())
      poolInstruments.corePoolSize.update(pool.getCorePoolSize())
    }

    def cleanup(): Unit =
      poolInstruments.cleanup()
  }

  private def forkJoinPoolSampler(name: String, tags: TagSet, pool: InstrumentedExecutorService[_]): ExecutorSampler = new ExecutorSampler {
    val poolInstruments = Instruments.forkJoinPool(name, tags)

    val taskCountSource = Counter.delta(() => pool.submittedTasks)
    val completedTaskCountSource = Counter.delta(() => pool.processedTasks)

    def sample(): Unit = {
      poolInstruments.poolMax.update(pool.maxThreads)
      poolInstruments.poolMin.update(pool.minThreads)
      poolInstruments.parallelism.update(pool.parallelism)
      poolInstruments.poolSize.record(pool.poolSize)
      poolInstruments.activeThreads.record(pool.activeThreads)
      taskCountSource.accept(poolInstruments.submittedTasks)
      completedTaskCountSource.accept(poolInstruments.processedTasks)
      poolInstruments.queuedTasks.record(pool.queuedTasks)
    }

    def cleanup(): Unit =
      poolInstruments.cleanup()
  }


  private def sampleTask(executorSampler: ExecutorSampler): Runnable = new Runnable {
    override def run(): Unit = executorSampler.sample()
  }

  private val delegatedExecutorField = {
    val field = DelegatedExecutor.getDeclaredField("e")
    field.setAccessible(true)
    field
  }

  def unwrap(delegatedExecutor: ExecutorService): ExecutorService =
    delegatedExecutorField.get(delegatedExecutor).asInstanceOf[ExecutorService]


  trait ForkJoinPoolMetrics[T]{
    def minThreads(pool: T): Int
    def maxThreads(pool: T): Int
    def activeThreads(pool: T): Int
    def poolSize(pool: T): Int
    def queuedTasks(pool: T): Int
    def parallelism(pool: T): Int
  }

  object ForkJoinPoolMetrics {
    def apply[T: ForkJoinPoolMetrics]: ForkJoinPoolMetrics[T] = implicitly
  }

  implicit class PoolMetricsProvider[A: ForkJoinPoolMetrics](pool: A) {
    def minThreads    = ForkJoinPoolMetrics[A].minThreads(pool)
    def maxThreads    = ForkJoinPoolMetrics[A].maxThreads(pool)
    def activeThreads = ForkJoinPoolMetrics[A].activeThreads(pool)
    def poolSize      = ForkJoinPoolMetrics[A].poolSize(pool)
    def queuedTasks   = ForkJoinPoolMetrics[A].queuedTasks(pool)
    def parallelism   = ForkJoinPoolMetrics[A].parallelism(pool)
  }

  implicit object JavaFJPMetrics extends ForkJoinPoolMetrics[JavaForkJoinPool] {
    override def minThreads(pool: JavaForkJoinPool): Int     = 0
    override def maxThreads(pool: JavaForkJoinPool): Int     = pool.getParallelism
    override def activeThreads(pool: JavaForkJoinPool): Int  = pool.getActiveThreadCount
    override def poolSize(pool: JavaForkJoinPool): Int       = pool.getPoolSize
    override def queuedTasks(pool: JavaForkJoinPool): Int    = pool.getQueuedSubmissionCount
    override def parallelism(pool: JavaForkJoinPool): Int    = pool.getParallelism
  }

  implicit object ScalaFJPMetrics extends ForkJoinPoolMetrics[ScalaForkJoinPool] {
    override def minThreads(pool: ScalaForkJoinPool): Int     = 0
    override def maxThreads(pool: ScalaForkJoinPool): Int     = pool.getParallelism
    override def activeThreads(pool: ScalaForkJoinPool): Int  = pool.getActiveThreadCount
    override def poolSize(pool: ScalaForkJoinPool): Int       = pool.getPoolSize
    override def queuedTasks(pool: ScalaForkJoinPool): Int    = pool.getQueuedSubmissionCount
    override def parallelism(pool: ScalaForkJoinPool): Int    = pool.getParallelism
  }

  def instrument(inner: ExecutorService): ExecutorService = inner match {
    case jfjp: JavaForkJoinPool   => new InstrumentedExecutorService(jfjp)(JavaFJPMetrics)
    case sfjp: ScalaForkJoinPool  => new InstrumentedExecutorService(sfjp)(ScalaFJPMetrics)
    case _                        => inner
  }

  class InstrumentedExecutorService[T <: ExecutorService](wrapped: T)(implicit metrics: ForkJoinPoolMetrics[T]) extends ExecutorService {


    private var submittedTasksCounter: LongAdder = new LongAdder
    private var completedTasksCounter: LongAdder = new LongAdder


    def submittedTasks = submittedTasksCounter.longValue()
    def processedTasks = completedTasksCounter.longValue()

    def minThreads    = wrapped.minThreads
    def maxThreads    = wrapped.maxThreads
    def activeThreads = wrapped.activeThreads
    def poolSize      = wrapped.poolSize
    def queuedTasks   = wrapped.queuedTasks

    def parallelism   = wrapped.parallelism

    override def submit(task: Runnable) = {
      submittedTasksCounter.increment
      wrapped.submit(wrapRunnable(task, completedTasksCounter))
    }

    override def submit[T](task: Runnable, result: T) =
    {
      submittedTasksCounter.increment
      wrapped.submit(wrapRunnable(task, completedTasksCounter), result)
    }

    override def submit[T](task: Callable[T]) = {
      submittedTasksCounter.increment
      wrapped.submit(wrapCallable(task, completedTasksCounter))
    }

    override def isTerminated = wrapped.isTerminated

    override def invokeAll[T](tasks: java.util.Collection[_ <: Callable[T]]) = {
      submittedTasksCounter.add(tasks.size())
      wrapped.invokeAll(tasks.asScala.map(task => wrapCallable(task, completedTasksCounter)).asJavaCollection)
    }

    override def invokeAll[T](tasks: java.util.Collection[_ <: Callable[T]], timeout: Long, unit: TimeUnit) = {
      submittedTasksCounter.add(tasks.size())
      wrapped.invokeAll(tasks.asScala.map(task => wrapCallable(task, completedTasksCounter)).asJavaCollection,timeout, unit)
    }

    override def awaitTermination(timeout: Long, unit: TimeUnit) = wrapped.awaitTermination(timeout, unit)

    override def shutdownNow() = wrapped.shutdownNow()

    override def invokeAny[T](tasks: java.util.Collection[_ <: Callable[T]]) = wrapped.invokeAny(tasks)

    override def invokeAny[T](tasks: java.util.Collection[_ <: Callable[T]], timeout: Long, unit: TimeUnit) = wrapped.invokeAny(tasks, timeout, unit)

    override def shutdown() = wrapped.shutdown()

    override def isShutdown = wrapped.isShutdown

    override def execute(command: Runnable) = wrapped.execute(wrapRunnable(command, completedTasksCounter))

    private def wrapCallable[T](task: Callable[T], metric: LongAdder): Callable[T] = new Callable[T] {
      override def call(): T = {
        val result = try {
          task.call()
        } finally metric.increment()
        result
      }
    }

    private def wrapRunnable(task: Runnable, metric: LongAdder): Runnable = new Runnable {
      override def run(): Unit = {
        try {
          task.run()
        } finally metric.increment()
      }
    }
  }

}


