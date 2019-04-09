
/*
 * =========================================================================================
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

package kamon.executors

import java.io.Closeable
import java.util.UUID

import kamon.testkit.{InstrumentInspection, MetricInspection}
import org.scalatest.{Matchers, WordSpec}
import java.util.concurrent.{ExecutorService, ForkJoinPool, ThreadPoolExecutor, Executors => JavaExecutors}

import kamon.Kamon
import kamon.tag.TagSet
import org.scalatest.concurrent.Eventually
import org.scalatest.time.{Millis, Span}

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future, Promise}


class ExecutorMetricsSpec extends WordSpec with Matchers with InstrumentInspection.Syntax with MetricInspection.Syntax with Eventually {


  implicit override val patienceConfig =
    PatienceConfig(timeout = scaled(Span(2000, Millis)), interval = scaled(Span(20, Millis)))

  class ExecutorMetrics(name: String, tpe: String) {


    def poolMin(reset: Boolean = false) = Metrics.pool(tpe, name).withTag("setting", "min").value(reset)
    def poolMax(reset: Boolean = false) = Metrics.pool(tpe, name).withTag("setting" , "max").value(reset)

    def threadsTotal(reset: Boolean = false) = Metrics.threads(tpe, name).withTag("state", "total").distribution(reset)
    def threadsActive(reset: Boolean = false) = Metrics.threads(tpe, name).withTag("state", "active").distribution(reset)

    def tasksSubmitted(reset: Boolean = false) = Metrics.tasks(tpe, name).withTag("state", "submitted").value(reset)
    def tasksCompleted(reset: Boolean = false) = Metrics.tasks(tpe, name).withTag("state", "completed").value(reset)

    def queue(reset: Boolean = false) = Metrics.queue(tpe, name).distribution(reset)

    def poolParallelism(reset: Boolean = false) = Metrics.pool(tpe, name).withTag("setting", "parallelism").value(reset)

    def poolCoreSize(reset: Boolean = false) = Metrics.pool(tpe, name).withTag("setting", "corePoolSize").value(reset)
  }

  "the ExecutorServiceMetrics" should {
    "register a SingleThreadPool, collect their metrics and remove it" in {
      val singleThreadPoolExecutor = JavaExecutors.newSingleThreadExecutor()
      val registeredPool = Executors.register("single-thread-pool", singleThreadPoolExecutor)

      Metrics.threadsMetric.tagValues("name")  should contain ("single-thread-pool")
      Metrics.threadsMetric.tagValues("type")  should contain ("tpe")

      registeredPool.close()
    }

    "register a ThreadPoolExecutor, collect their metrics and remove it" in {
      val threadPoolExecutor = JavaExecutors.newCachedThreadPool()
      val registeredPool = Executors.register("thread-pool-executor", threadPoolExecutor)

      Metrics.threadsMetric.tagValues("name")  should contain ("thread-pool-executor")
      Metrics.threadsMetric.tagValues("type")  should contain ("tpe")

      registeredPool.close()
    }

    "register a ScheduledThreadPoolExecutor, collect their metrics and remove it" in {
      val scheduledThreadPoolExecutor = JavaExecutors.newSingleThreadScheduledExecutor()
      val registeredPool = Executors.register("scheduled-thread-pool-executor", scheduledThreadPoolExecutor)

      Metrics.threadsMetric.tagValues("name")  should contain ("scheduled-thread-pool-executor")
      Metrics.threadsMetric.tagValues("type")  should contain ("tpe")

      registeredPool.close()
    }

    "register a ForkJoinPool, collect their metrics and remove it" in {
      val javaForkJoinPool = Executors.instrument(JavaExecutors.newWorkStealingPool())
      val registeredForkJoin = Executors.register("java-fork-join-pool", javaForkJoinPool)

      Metrics.threadsMetric.tagValues("name")  should contain ("java-fork-join-pool")
      Metrics.threadsMetric.tagValues("type")  should contain ("fjp")

      registeredForkJoin.close()
    }

    "register a Scala ForkJoinPool, collect their metrics and remove it" in {
      val scalaForkJoinPool = Executors.instrument(new scala.concurrent.forkjoin.ForkJoinPool(10))
      val registeredForkJoin = Executors.register("scala-fork-join-pool", scalaForkJoinPool)

      Metrics.threadsMetric.tagValues("name")  should contain ("scala-fork-join-pool")
      Metrics.threadsMetric.tagValues("type")  should contain ("fjp")

      registeredForkJoin.close()
    }

  }

  def setupTestPool(executor: ExecutorService): (ExecutorService, ExecutorMetrics, Closeable) = {
    val typeTag = executor match {
      case javaFjp:ForkJoinPool                             => "fjp"
      case scalaFjp: scala.concurrent.forkjoin.ForkJoinPool => "fjp"
      case tpe:ThreadPoolExecutor                           => "tpe"
    }
    val pool = Executors.instrument(executor)
    val name = s"testExecutor-${UUID.randomUUID()}"
    val registered = Executors.register(name, pool)
    val metrics = new ExecutorMetrics(name, typeTag)
    (pool, metrics, registered)
  }



  def commonExecutorMetrics(executor: Int => ExecutorService, size: Int) = {
    "track settings" in {
      val (pool, metrics, registration) = setupTestPool(executor(size))
      eventually(metrics.poolMax() should be (size))
      registration.close()
    }

    "track tasks" in {
      val (pool, metrics, registration) = setupTestPool(executor(size))
      val semaphore = Promise[String]()

      eventually {
        metrics.tasksSubmitted()      should be (0)
        metrics.tasksCompleted()      should be (0)
      }

      val blockedTask = new Runnable {
        override def run(): Unit = {
          Await.result(semaphore.future, Duration.Inf)
          ()
        }}

      pool.submit(blockedTask)
      eventually {
        (metrics.tasksSubmitted(), metrics.tasksCompleted()) should be (1, 0)
      }

      semaphore.success("done")
      eventually {
        (metrics.tasksSubmitted(), metrics.tasksCompleted()) should be (1, 1)
      }

      (1 to 10).foreach(_ => pool.submit(blockedTask))
      eventually {
        (metrics.tasksSubmitted(), metrics.tasksCompleted()) should be (11, 11)
      }
      registration.close()
    }

    "track threads" in {
      val (pool, metrics, registration) = setupTestPool(executor(2))

      eventually {
        metrics.threadsTotal().max should be (0)
        metrics.threadsActive().max should be (0)
      }

      Future(
        (1 to 10000).foreach(_ => pool.submit(new Runnable {
          override def run(): Unit = Thread.sleep(1)
        }))
      )(scala.concurrent.ExecutionContext.global)

      eventually {
        metrics.threadsActive().max should be (2)
        metrics.threadsTotal().max should be (2)
      }
      registration.close()
    }

    "track queue" in {
      val (pool, metrics, registration) = setupTestPool(executor(size))

      val semaphore = Promise[String]()
      val blockedTask = new Runnable {
        override def run(): Unit = {
          Await.result(semaphore.future, Duration.Inf)
          ()
        }}

      eventually(metrics.queue().max should be (0))

      (1 to 100).foreach(_ => pool.submit(blockedTask))

      pool.submit(blockedTask)
      eventually {
        val queue = metrics.queue().max
        val activeThreads = metrics.threadsActive().max
        metrics.queue().max should be >= (100 - activeThreads)
      }

      registration.close()
    }
  }

  def fjpMetrics(executor: Int => ExecutorService, size: Int) = {
    val (pool, metrics, registration) = setupTestPool(executor(size))
    "track FJP specific matrics" in {
      eventually {
        metrics.poolParallelism() should be (size)
        metrics.poolMin() should be (0)
      }
      registration.close()
    }
  }
  def tpeMetrics(executor: Int => ExecutorService, size: Int) = {
    val (pool, metrics, registration) = setupTestPool(executor(size))
    "track TPE specific matrics" in {
      eventually {
        metrics.poolCoreSize() should be (size)
        metrics.poolMin() should be (size)
        registration.close()

      }
    }
  }

  "Executor service" when {
    "backed by Java FJP" should {
      behave like commonExecutorMetrics(JavaExecutors.newWorkStealingPool(_), 10)
      behave like fjpMetrics(JavaExecutors.newWorkStealingPool(_), 1)
    }
    "backed by Scala FJP" should {
      behave like commonExecutorMetrics(new scala.concurrent.forkjoin.ForkJoinPool(_), 10)
    }
    "backed by TPE" should {
      behave like commonExecutorMetrics(JavaExecutors.newFixedThreadPool(_), 10)
      behave like tpeMetrics(JavaExecutors.newFixedThreadPool(_), 10)
    }
  }

}

