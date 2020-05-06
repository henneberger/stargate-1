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

import java.util.concurrent.{Executors, TimeUnit}

import com.fasterxml.jackson.databind.{ObjectMapper, SerializationFeature}

import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration.Duration
import scala.jdk.CollectionConverters._
import scala.util.Try

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
      case x => x
    }
  }
  def scalaToJava(x: Object): Object = {
    x match {
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

  def await[T](f: Future[T]): Try[T] = Try(Await.result(f, Duration.Inf))

  def newCachedExecutor: ExecutionContext = ExecutionContext.fromExecutor(Executors.newCachedThreadPool)
}
