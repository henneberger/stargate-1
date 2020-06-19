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

import java.util.concurrent.{Callable, CompletableFuture, Executors, ScheduledExecutorService, ScheduledFuture, TimeUnit}

import com.fasterxml.jackson.databind.{ObjectMapper, SerializationFeature}

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.jdk.CollectionConverters._
import scala.jdk.FutureConverters._
import scala.util.{Success, Try}

package object util {

  val objectMapper = new ObjectMapper
  val indentedObjectMapper = {
    val om = new ObjectMapper
    om.configure(SerializationFeature.INDENT_OUTPUT, true)
    om
  }
  def fromJson(s: String): Object = javaToScala(objectMapper.readValue(s, classOf[Object]))
  def toJson(o: Object): String = objectMapper.writeValueAsString(scalaToJava(o))
  def toPrettyJson(o: Object): String = indentedObjectMapper.writeValueAsString(scalaToJava(o))

  def enumerationNames(enum: Enumeration): Map[String, enum.Value] = enum.values.iterator.map(v => (v.toString,v)).toMap

  def javaToScala(x: Object): Object = {
    x match {
      case x: java.util.List[Object] =>  x.asScala.map(javaToScala).toList
      case x: java.util.Map[Object, Object] => x.asScala.map((kv:(Object,Object)) => (javaToScala(kv._1), javaToScala(kv._2))).toMap
      case x: List[Object] => x.map(javaToScala)
      case x: Map[Object, Object] => x.map((kv: (Object,Object)) => (javaToScala(kv._1), javaToScala(kv._2)))
      case x => x
    }
  }
  def scalaToJava(x: Object): Object = {
    x match {
      case x: java.util.List[Object] =>  x.asScala.map(scalaToJava).asJava
      case x: java.util.Map[Object, Object] => x.asScala.map((kv:(Object,Object)) => (scalaToJava(kv._1), scalaToJava(kv._2))).toMap.asJava
      case x: List[Object] =>  x.map(scalaToJava).asJava
      case x: Map[Object, Object] => (x.map((kv:(Object,Object)) => (scalaToJava(kv._1), scalaToJava(kv._2)))).asJava : java.util.Map[Object,Object]
      case x => x
    }
  }

  def retry[T](value: => T, remaining: Duration, backoff: Duration): Try[T] = {
    val t0 = System.nanoTime()
    val result = Try(value)
    val t1 = System.nanoTime()
    val nextRemaining = remaining - Duration(t1 - t0, TimeUnit.NANOSECONDS) - backoff
    if(result.isSuccess || nextRemaining._1 < 0) {
      result
    } else {
      Thread.sleep(backoff.toMillis)
      retry(value, nextRemaining, backoff)
    }
  }

  def asScala[T](future: java.util.concurrent.Future[T], backoff: Duration, scheduler: ScheduledExecutorService): Future[T] = {
    val completableFuture = new CompletableFuture[T]()
    val check: Object => Runnable = (self: Object) => () => {
      if(future.isDone) {
        val tryget = Try(future.get)
        if(tryget.isSuccess) {
          completableFuture.complete(tryget.get)
        } else {
          completableFuture.completeExceptionally(tryget.failed.get)
        }
      } else {
        val check = self.asInstanceOf[Object => Runnable]
        scheduler.schedule(check(check), backoff._1, backoff._2)
      }
    }
    check(check).run()
    completableFuture.asScala
  }
  def retry[T](value: ()=>Future[T], remaining: Duration, backoff: Duration, scheduler: ScheduledExecutorService): Future[T] = {
    val executor = ExecutionContext.fromExecutor(scheduler)
    val t0 = System.nanoTime()
    val future: Future[Future[T]] = value().transform(result => {
      val t1 = System.nanoTime()
      val nextRemaining = remaining - Duration(t1 - t0, TimeUnit.NANOSECONDS) - backoff
      if(result.isSuccess || nextRemaining._1 < 0) {
        Success(Future.fromTry(result))
      } else {
        val retryCallable: Callable[Future[T]] = () => retry(value, nextRemaining, backoff, scheduler)
        val retryFuture: ScheduledFuture[Future[T]] = scheduler.schedule(retryCallable, backoff._1, backoff._2)
        Success(asScala(retryFuture, Duration.apply(math.max(1, backoff._1/5), backoff._2), scheduler).flatten)
      }
    })(executor)
    future.flatten
  }
  def retry[T](value: ()=>Future[T], remaining: Duration, backoff: Duration): Future[T] = retry(value, remaining, backoff, Executors.newScheduledThreadPool(1))

  def await[T](f: Future[T]): Try[T] = Try(Await.result(f, Duration.Inf))

  def sequence[T](fs: List[Future[T]], executor: ExecutionContext): Future[List[T]] = {
    implicit val ec: ExecutionContext = executor
    Future.sequence(fs)
  }
  def allDefined[T](os: List[Option[T]]): Option[List[T]] = if(os.contains(None)) None else Some(os.flatten)

  def flattenFOF[T](fof: Future[Option[Future[T]]], executor: ExecutionContext): Future[Option[T]] = fof.flatMap(of => of.map(f => f.map(Some.apply)(executor)).getOrElse(Future.successful(None)))(executor)
  def flattenFOFO[T](fofo: Future[Option[Future[Option[T]]]], executor: ExecutionContext): Future[Option[T]] = fofo.flatMap(ofo => ofo.getOrElse(Future.successful(None)))(executor)
  def flattenFOLFO[T](folfo: Future[Option[List[Future[Option[T]]]]], executor: ExecutionContext): Future[Option[List[T]]] = {
    folfo.flatMap(olfo => olfo.map(lfo => util.sequence(lfo, executor).map(lo => util.allDefined(lo))(executor)).getOrElse(Future.successful(None)))(executor)
  }
  def flattenFOLFOL[T](folfol: Future[Option[List[Future[Option[List[T]]]]]], executor: ExecutionContext): Future[Option[List[T]]] = {
    flattenFOLFO(folfol, executor).map(_.map(_.flatten))(executor)
  }
  def flattenFLF[T](flf: Future[List[Future[T]]], executor: ExecutionContext): Future[List[T]] = flf.flatMap(lf => sequence(lf, executor))(executor)

  def newCachedExecutor: ExecutionContext = ExecutionContext.fromExecutor(Executors.newCachedThreadPool)
}
