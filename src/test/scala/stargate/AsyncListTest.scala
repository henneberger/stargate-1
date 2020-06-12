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

package stargate

import org.junit.Test
import stargate.util.AsyncList

import scala.concurrent.{ExecutionContext, Future}
import scala.util.Random


class AsyncListTest {

  implicit val executor: ExecutionContext = ExecutionContext.parasitic

  def delayed[T](sleep: Long, value: T): Future[T] = Future.apply({Thread.sleep(sleep); value})(util.newCachedExecutor)

  def delayedList[T](sleep: Long, makeValue: Long => T, remaining: Long): AsyncList[T] = {
    AsyncList(() => {
      if(remaining == 0) {
        Future.successful(None)
      } else {
        delayed(sleep, makeValue(remaining)).map(head => Some(head, delayedList(sleep, makeValue, remaining - 1)))
      }
    })
  }
  def delayedList(sleep: Long, remaining: Long): AsyncList[Long] = delayedList(sleep, identity, remaining)


  def within(value: Long, expected: Long, tolerance: Long): Unit = assert(expected - tolerance <= value && value <= expected + tolerance, value + " < " + expected + "+/-" + tolerance)

  @Test
  def testLazyMap: Unit = {
    val sleep = 500
    val size = 10
    val tolerance = 200
    val t0 = System.currentTimeMillis()
    val al0 = delayedList(sleep, size)
    val t1 = System.currentTimeMillis()
    assert(t1 - t0 < tolerance)
    val al1 = al0.map(_ + 1, executor)
    val t2 = System.currentTimeMillis()
    assert(t2 - t1 < tolerance)
    val l1 = util.await(al1.toList(executor))
    val t3 = System.currentTimeMillis()
    within(t3 - t2, sleep*size, 2*sleep)
    val l0 = util.await(al0.toList(executor))
    val t4 = System.currentTimeMillis()
    assert(t4 - t3 < tolerance)
    assert(l1.get == l0.get.map(_ + 1))
  }

  @Test
  def testLazyAppend: Unit = {
    val sleep = 200
    val size = 10
    val tolerance = 200
    val t0 = System.currentTimeMillis()
    val al0a = delayedList(sleep, size)
    val al0b = delayedList(sleep, size)
    val al0 = AsyncList.append(al0a, al0b, executor)
    val t1 = System.currentTimeMillis()
    assert(t1 - t0 < tolerance)
    val l0 = util.await(al0.toList(executor))
    val t2 = System.currentTimeMillis()
    within(t2 - t1, sleep*size*2, sleep)
    val l0a = util.await(al0a.toList(executor)).get
    val l0b = util.await(al0b.toList(executor)).get
    val t3 = System.currentTimeMillis()
    assert(t3 - t2 < tolerance)
    assert(l0.get == (l0a ++ l0b))
  }

  @Test
  def testLazyFlatten: Unit = {
    val sleep = 50
    val size = 10
    val tolerance = 200
    val t0 = System.currentTimeMillis()
    val al0 = delayedList(sleep, rem => delayedList(sleep, rem), size)
    val t1 = System.currentTimeMillis()
    assert(t1 - t0 < tolerance)
    val al1 = AsyncList.flatten(al0, executor)
    val t2 = System.currentTimeMillis()
    assert(t2 - t1 < tolerance)
    val l1 = util.await(al1.toList(executor))
    val t3 = System.currentTimeMillis()
    val l0 = util.await(al0.toList(executor))
    val t4 = System.currentTimeMillis()
    within(t3 - t2, sleep*(l1.get.length + l0.get.length), 10*sleep)
    assert(t4 - t3 < tolerance)
    val l0flat = l0.get.flatMap(al => util.await(al.toList(executor)).get)
    val t5 = System.currentTimeMillis
    assert(t5 - t4 < tolerance)
    assert(l0flat == l1.get)
  }

  @Test
  def testlazyDedupe: Unit = {
    val sleep = 1
    val size = 2000
    val tolerance = 200
    val executor = util.newCachedExecutor
    val t0 = System.currentTimeMillis()
    val al0 = delayedList(sleep, _ => Random.nextInt(3), size)
    val t1 = System.currentTimeMillis()
    assert(t1 - t0 < tolerance)
    val al1 = al0.dedupe(executor)
    val t2 = System.currentTimeMillis()
    assert(t2 - t1 < tolerance)
    val l1 = util.await(al1.toList(executor)).get
    val t3 = System.currentTimeMillis()
    within(t3 - t2, sleep*size, size/100*tolerance)
    assert(l1.size == 3)
  }


}
