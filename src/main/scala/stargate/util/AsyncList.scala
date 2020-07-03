/*
 * Copyright DataStax, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package stargate.util

import java.util.concurrent.{Executor, Executors}

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}


class AsyncList[T] private (private val getValue: () => Future[Option[(T, AsyncList[T])]]) {

  import AsyncList._

  var valueCache: Option[Future[Option[(T, AsyncList[T])]]] = None

  def value: Future[Option[(T, AsyncList[T])]] = {
    if(valueCache.isDefined) {
      valueCache.get
    } else {
      this.synchronized {
        if(valueCache.isEmpty) {
          valueCache = Some(this.getValue())
        }
        valueCache.get
      }
    }
  }

  def isEmpty(executor: ExecutionContext): Future[Boolean] = {
    value.map(_.isEmpty)(executor)
  }

  def maybeHead(executor: ExecutionContext): Future[Option[T]] = {
    value.map(_.map(_._1))(executor)
  }

  def head(executor: ExecutionContext): Future[T] = {
    value.map(_.get._1)(executor)
  }

  def tail(executor: ExecutionContext): AsyncList[T] = {
    unfuture(value.map(_.get._2)(executor), executor)
  }

  def map[T2](f: T=>T2, executor: ExecutionContext): AsyncList[T2] = {
    new AsyncList(() => {
      this.value.map(maybeList => {
        maybeList.map(list => {
          (f(list._1), list._2.map(f, executor))
        })
      })(executor)
    })
  }

  def filter(f: T=>Boolean, executor: ExecutionContext): AsyncList[T] = {
    new AsyncList[T](() => {
      stargate.util.flattenFOFO(this.value.map(maybeList => {
        maybeList.map(list => {
          val tail = list._2.filter(f, executor)
          if(f(list._1)) {
            Future.successful(Some((list._1, tail)))
          } else {
            tail.value
          }
        })
      })(executor), executor)
    })
  }

  // note: causes whole list to be read eagerly
  def foldLeft[T2](init: T2)(f: (T2, T) => T2, executor: ExecutionContext): Future[T2] = {
    this.value.flatMap(maybeList => {
      maybeList.map(list => {
        val next = f(init, list._1)
        list._2.foldLeft(next)(f, executor)
      }).getOrElse(Future.successful(init))
    })(executor)
  }

  def flatMap[T2](f: T=>AsyncList[T2], executor: ExecutionContext): AsyncList[T2] = {
    flatten(this.map(f, executor), executor)
  }

  def splitAt(index: Int, executor: ExecutionContext): (Future[List[T]], AsyncList[T]) = {
    val wrapped = if(index <= 0) {
      Future.successful(List.empty, this)
    } else {
      this.value.flatMap(maybeList => {
        maybeList.map(list => {
          val recurse = list._2.splitAt(index - 1, executor)
          recurse._1.map(take => (list._1 +: take, recurse._2))(executor)
        }).getOrElse(Future.successful((List.empty, this)))
      })(executor)
    }
    (wrapped.map(_._1)(executor), unfuture(wrapped.map(_._2)(executor), executor))
  }

  def take(index: Int, executor: ExecutionContext): Future[List[T]] = this.splitAt(index, executor)._1
  def drop(index: Int, executor: ExecutionContext): AsyncList[T] = this.splitAt(index, executor)._2

  def span(predicate: T => Boolean, executor: ExecutionContext): (Future[List[T]],AsyncList[T]) = {
    val wrapped = this.value.flatMap(maybeList => {
      maybeList.map(list => {
        if(predicate(list._1)) {
          val recurse = list._2.span(predicate, executor)
          recurse._1.map(take => (list._1 +: take, recurse._2))(executor)
        } else {
          Future.successful(List.empty, this)
        }
      }).getOrElse(Future.successful(List.empty, this))
    })(executor)
    (wrapped.map(_._1)(executor), unfuture(wrapped.map(_._2)(executor), executor))
  }

  def dedupe(executor: ExecutionContext): AsyncList[T] = {
    val set = new java.util.concurrent.ConcurrentHashMap[Option[T], Option[T]]()
    this.filter(x => {
      val option = Option(x)
      set.putIfAbsent(option, option) == null
    }, executor)
  }

  def toList(executor: ExecutionContext): Future[List[T]] = {
    this.value.flatMap(maybeList => {
      maybeList.map(list => {
        list._2.toList(executor).map(tail => list._1 +: tail)(executor)
      }).getOrElse(Future.successful(List.empty))
    })(executor)
  }

  def length(executor: ExecutionContext): Future[Int] = {
    this.foldLeft(0)((sum, _) => sum + 1, executor)
  }

  override def toString: String = {
    val threadPool = Executors.newFixedThreadPool(1)
    implicit val ec = ExecutionContext.fromExecutor(threadPool)
    val result = Await.result(this.toList(ec).map("Async" + _.toString), Duration.Inf)
    threadPool.shutdownNow()
    result
  }
}

object AsyncList {

  def empty[T] = new AsyncList[T](() => Future.successful(None))

  def apply[T](getValue: () => Future[Option[(T,AsyncList[T])]]): AsyncList[T] = new AsyncList[T](getValue)

  def singleton[T](single: () => Future[T], executor: ExecutionContext): AsyncList[T] = {
    pushLazy(single, empty[T], executor)
  }
  def singleton[T](single: T): AsyncList[T] = {
    new AsyncList[T](() => Future.successful(Some((single, empty))))
  }

  def pushLazy[T](head: () => Future[T], tail: AsyncList[T], executor: ExecutionContext): AsyncList[T] = {
    new AsyncList[T](() => head().map(head => Some(head, tail))(executor))
  }
  def push[T](head: T, tail: AsyncList[T], executor: ExecutionContext): AsyncList[T] = {
    pushLazy(() => Future.successful(head), tail, executor)
  }

  def append[T](a: AsyncList[T], b: AsyncList[T], executor: ExecutionContext): AsyncList[T] = {
    new AsyncList(() => {
      a.value.flatMap(maybeList => {
        maybeList.map(list => {
          Future.successful(Some((list._1, append(list._2, b, executor))))
        }).getOrElse(b.value)
      })(executor)
    })
  }

  def flatten[T](lists: AsyncList[AsyncList[T]], executor: ExecutionContext): AsyncList[T] = {
    new AsyncList(() => {
      lists.value.flatMap(maybeListOfLists => {
        maybeListOfLists.map(listOfLists => {
          listOfLists._1.value.flatMap(maybeInner => {
            maybeInner.map(inner => {
              Future.successful(Some((inner._1, flatten(push(inner._2, listOfLists._2, executor), executor))))
            }).getOrElse(flatten(listOfLists._2, executor).value)
          })(executor)
        }).getOrElse(Future.successful(None))
      })(executor)
    })
  }

  def contiguousGroups[T, K](list: AsyncList[T], key: T=>K, executor: ExecutionContext): AsyncList[List[T]] = {
    AsyncList(() => stargate.util.flattenFOF(list.value.map(_.map(head_tail => {
      val headKey = key(head_tail._1)
      val (matching, remaining) = head_tail._2.span(x => key(x) == headKey, executor)
      matching.map(m => (head_tail._1 +: m, contiguousGroups(remaining, key, executor)))(executor)
    }))(executor), executor))
  }

  def filterSome[T](list: AsyncList[Option[T]], executor: ExecutionContext): AsyncList[T] = {
    list.filter(_.isDefined, executor).map(_.get, executor)
  }

  def unfuture[T](futureList: Future[AsyncList[T]], executor: ExecutionContext): AsyncList[T] = {
    new AsyncList(() => futureList.flatMap(_.value)(executor))
  }
  def unfuture[T](futureList: AsyncList[Future[T]], executor: ExecutionContext): AsyncList[T] = {
    new AsyncList(() => {
      futureList.value.flatMap(maybeList => {
        maybeList.map(list => {
          list._1.map(value => Some((value, unfuture(list._2, executor))))(executor)
        }).getOrElse(Future.successful(None))
      })(executor)
    })
  }

  def fromList[T](list: List[T]): AsyncList[T] = {
    if(list.isEmpty) {
      empty[T]
    } else {
      new AsyncList[T](() => Future.successful(Some((list.head, fromList(list.tail)))))
    }
  }
}


