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

package kamon.executors

import kamon.metric.{Counter, Gauge, Histogram}
import kamon.tag.TagSet
import kamon.Kamon

object Metrics {

  def pool(tpe: String, name: String): Gauge = Kamon.gauge("executor.pool").withTag("type", tpe).withTag("name", name)
  def threads(tpe: String, name: String): Histogram = Kamon.histogram("executor.threads").withTag("type", tpe).withTag("name", name)
  def tasks(tpe: String, name: String): Counter = Kamon.counter("executor.tasks").withTag("type", tpe).withTag("name", name)
  def queue(tpe: String, name: String): Histogram = Kamon.histogram("executor.queue").withTag("type", tpe).withTag("name", name)

  def forkJoinPool(name: String, tags: TagSet): ForkJoinPoolMetrics = {
    val Pool = pool("fjp", name)
    val Threads = threads("fjp", name)
    val Tasks = tasks("fjp", name)
    val Queue = queue("fjp", name)

    ForkJoinPoolMetrics(
      Pool.withTag("setting", "min"),
      Pool.withTag("setting", "max"),
      Threads.withTag("state", "total"),
      Threads.withTag("state", "active"),
      Tasks.withTag("state", "submitted"),
      Tasks.withTag("state", "completed"),
      Queue,
      Pool.withTag("setting", "parallelism")
    )
  }

  case class ForkJoinPoolMetrics(
    poolMin: Gauge,
    poolMax: Gauge,
    poolSize: Histogram,
    activeThreads: Histogram,
    submittedTasks: Counter,
    processedTasks: Counter,
    queuedTasks: Histogram,
    parallelism: Gauge
  ) {

    def cleanup(): Unit = {
      poolMin.remove()
      poolMax.remove()
      poolSize.remove()
      activeThreads.remove()
      submittedTasks.remove()
      processedTasks.remove()
      queuedTasks.remove()
      parallelism.remove()
    }

  }


  def threadPool(name: String, tags: TagSet): ThreadPoolMetrics = {
    val poolType = "tpe"
    val Pool = pool(poolType, name)
    val Threads = threads(poolType, name)
    val Tasks = tasks(poolType, name)
    val Queue = queue(poolType, name)


    ThreadPoolMetrics(
      Pool.withTag("setting", "min"),
      Pool.withTag("setting", "max"),
      Threads.withTag("state", "total"),
      Threads.withTag("state", "active"),
      Tasks.withTag("state","submitted"),
      Tasks.withTag("state", "completed"),
      Queue,
      Pool.withTag("setting", "corePoolSize")
    )
  }


  case class ThreadPoolMetrics(
    poolMin: Gauge,
    poolMax: Gauge,
    poolSize: Histogram,
    activeThreads: Histogram,
    submittedTasks: Counter,
    processedTasks: Counter,
    queuedTasks: Histogram,
    corePoolSize: Gauge
  ) {

    def cleanup(): Unit = {
      poolMin.remove()
      poolMax.remove()
      poolSize.remove()
      activeThreads.remove()
      submittedTasks.remove()
      processedTasks.remove()
      queuedTasks.remove()
      corePoolSize.remove()
    }

  }




}
